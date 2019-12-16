package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64 // 当前读取数据的位置
	writePos     int64 // 当前写入数据的位置
	readFileNum  int64 // 当前正在读取的文件序号
	writeFileNum int64 // 当前正在写入的文件序号
	depth        int64 // 未读消息条数；

	sync.RWMutex // 同步用；上面所有信息的修改都需要同步来进行

	// instantiation time metadata
	name            string // 队列名字
	dataPath        string
	maxBytesPerFile int64         // currently this cannot change once created
	minMsgSize      int32         // 每条消息大小的最小值
	maxMsgSize      int32         // 每条消息大小的最大值
	syncEvery       int64         // number of writes per fsync
	syncTimeout     time.Duration // duration of time per fsync
	exitFlag        int32         // 退出的标志
	needSync        bool          // 是否需要将writeBuf中的数据刷写到文件中

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64 // 下次读取数据的位置
	nextReadFileNum int64 // 下次读取的文件序号

	readFile  *os.File      // 当前读取的文件句柄
	writeFile *os.File      // 当前写入的文件句柄
	reader    *bufio.Reader // 读buffer
	writeBuf  bytes.Buffer  // 写buffer

	// 下面的这些channel都是阻塞channel
	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	/* 这种chan, responseChan的结构，很像request、response；
	在等chan中有请求过来，然后进行处理，处理之后把结果丢给responseChan返回；
	*/
	writeChan         chan []byte // 写入的channel
	writeResponseChan chan error  // 写入返回值的channel
	emptyChan         chan int    // 接收清空命令的chanel
	emptyResponseChan chan error  // 清空命令返回值的channel
	exitChan          chan int    // 接受退出命令的channel
	exitSyncChan      chan int    // 退出命令返回值的channel

	logf AppLogFunc // log util
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,     // disk queue的名称，主要用于log和文件标识
		dataPath:          dataPath, // 路径，以后用于存放meta_data
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,        // 最小消息长度
		maxMsgSize:        maxMsgSize,        // 最大消息长度
		readChan:          make(chan []byte), // 初始化各种channel；所有的channel都是阻塞式的
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,   // 每过多久进行一次同步
		syncTimeout:       syncTimeout, // 同步的timeout
		logf:              logf,        // 打印log
	}
	// no need to lock here, nothing else could possibly be touching this instance
	// 如果以前存在过同名的queue；则从文件中恢复上次退出时的状态
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}
	// 启动queue
	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
// put操作是同步的，需要加锁来进行
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	// 写入和写入的返回值是分开两个chan来进行的；
	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()
	// 是否正在退出
	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	// 重置；删除所有的数据文件
	err := d.skipToNextRWFile()
	// 删除当前的meta文件
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

// 清空diskQueue内的所有消息，并重置 depth, readPos, readFileNum, writePos, readFileNum信息
func (d *diskQueue) skipToNextRWFile() error {
	var err error
	// 关闭当前正在读取的文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	// 关闭当前正在写入的文件
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	// 依次删除所有的数据文件
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}
	// 重置各种信息，将depth, readPos, readFileNum, writePos, writeFileNum 归零
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
// 根据规定的消息写入格式，从文件中读取一条消息
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	// 两种情况这个readFile == nil
	// 	1. 当读完上一个文件的时候，会关闭掉上一个文件的句柄。并在下面打开应该读取的新文件。
	//	2. 当diskQueue启动的时候，会打开相应的数据文件，并查找到应该进行读的位置。
	if d.readFile == nil {
		// 获取当前应该打开的文件名
		curFileName := d.fileName(d.readFileNum)
		// 打开文件
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)
		// 当重新启动的时候readPos是上一次的位置
		if d.readPos > 0 {
			// 找到应该读取的位置
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		// 创建一个BufferReader
		d.reader = bufio.NewReader(d.readFile)
	}
	// 首先读取4个字节的数据，作为消息长度
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	// 验证消息长度是否合法
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}
	// 根据消息的长度创建相应的readBuf，并去读msgSize个字节到readBuf中
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	// 本次总共读取的字节数为4个字节消息长度 + msgSize个字节的消息体
	totalBytes := int64(4 + msgSize)
	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	// 下一次应该读取数据的位置和文件号
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	// 如果下一次应该读取的数据超过了单个文件的长度，则从下一个文件中进行读取
	// 这里暴露出一个问题；每个文件的maxBytesPerFile的大小是固定的；如果中途修改了的话不就gg了吗？
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
// 根据规定的消息写入格式，写入一条消息。
func (d *diskQueue) writeOne(data []byte) error {
	var err error
	// 两种情况这个writeFile == nil
	// 	1. 当上一个文件写满了的时候，会关闭掉上一个文件的句柄。在下面创建新的数据文件。
	//	2. 当diskQueue启动的时候，会打开相应的数据文件，并查找到应该进行写的位置。
	if d.writeFile == nil {
		// 根据当前的writeFileNum来创建新的数据文件
		curFileName := d.fileName(d.writeFileNum)
		// 打开文件
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			// 查找到应该进行写的位置
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}
	// 首先获取4个字节的消息长度，并验证消息长度是否在合法的范围内。
	dataLen := int32(len(data))
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	// 清空writeBuf，以便进行写入
	d.writeBuf.Reset()
	// 在这里先把数据长度按照大段存储的方式写入writeBuf中
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}
	// 在这里把data写入到writeBuf中
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}
	// only write to the file once
	// 在这里才将writeBuf中的数据一次性写入到磁盘中。
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	// 这里的dataLen + 4是因为上面先写入了dataLen，由于dataLen是32位整型，4个byte，
	// 所以这里先加上属于数据长度的4个字节
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	// diskQueue中的未读数据长度+1
	atomic.AddInt64(&d.depth, 1)

	// 如果当前 write index 超过了设定的文件的最大长度，则下次开始写新文件。
	if d.writePos >= d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		// 每次准备写新文件的时候都会进行sync操作。
		err = d.sync()
		if err != nil {
			// 这里的错误最后一起返回
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}
		// 关闭文件
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}
	// 保存metaData
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
// 从文件中恢复depth, readFileNum, readPos, writeFileNum, writePos信息。
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	// 获取文件名
	fileName := d.metaDataFileName()
	// 打开文件
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	// 从文件中按照特定的格式读取数据
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n", &depth, &d.readFileNum, &d.readPos, &d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	// 将获取到的meta数据保存下来
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum // 初试状态下 nextReadFileNum = readFileNum
	d.nextReadPos = d.readPos         // 初始状态下 nextReadPos = readPos
	return nil
}

// persistMetaData atomically writes state to the filesystem
// 将depth, readFileNum, readPos, writeFileNum, writePos信息持久化到文件中
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	// 获取metaFileName
	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	// 把数据写入到临时文件中
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n", atomic.LoadInt64(&d.depth), d.readFileNum, d.readPos, d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	// 重命名， CopyOnWrite策略，防止污染原来的metaFile
	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

// meta文件名称
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

// 数据文件名称
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// 检查是否读取到了末尾
func (d *diskQueue) checkTailCorruption(depth int64) {
	// 这个表示还有未读的数据
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}
	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR, "DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...", d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR, "DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...", d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}
	// 在这里判断异常情况
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		// 读取文件序号>写入文件序号
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR, "DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...", d.name, d.readFileNum, d.writeFileNum)
		}
		// 读取位置大于>写入位置
		if d.readPos > d.writePos {
			d.logf(ERROR, "DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...", d.name, d.readPos, d.writePos)
		}
		// 重置disk-queue
		d.skipToNextRWFile()
		d.needSync = true
	}
}

// 从文件中读取一条message过后，把readPos和readFileNum向后挪动
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true
		// 老数据被消费的了之后不留一个记录了吗？在这里把老数据都清理了
		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}

	d.checkTailCorruption(depth)
}

// 处理读取数据的问题
func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	// 如果读取的文件和当前正在写入的文件是同一个文件；则关闭当前文件，从下一个文件开始写；
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		// 从下一个文件开始写
		d.writeFileNum++
		d.writePos = 0
	}
	// 把当前正在读取的这个文件命名为.bad后缀
	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN, "DISKQUEUE(%s) jump to next file and saving bad file as %s", d.name, badRenameFn)
	// 进行重命名
	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s", d.name, badFn, badRenameFn)
	}
	// 从下一个文件开始继续读取
	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	// 同步一下文件
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
// 核心
func (d *diskQueue) ioLoop() {
	var dataRead []byte // 读取出的数据
	var err error       // 错误
	var count int64     // 用来记录读取、写入了多少条消息；每一次操作这个count都会++
	var r chan []byte   // 用来暂存d.readChan
	// 创建一个sync操作的ticker
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		// 每读syncEvery次消息，会进行一次sync操作。
		if count == d.syncEvery {
			d.needSync = true
		}
		// 每写入count条消息后；才将writeFile中的内容同步到文件；
		if d.needSync {
			// 将d.writeFile中的文件和meta信息刷写到磁盘。
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			// 刷写过后count计数清零
			count = 0
		}
		// 这个说明有未读的数据
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 当nextReadPos != readPos的时候，说明readChan中还有未处理的消息。不用readOne
			// 当nextReadPos == readPos的时候，说明readChan中已经没有未处理的消息了，需要进行readOne
			if d.nextReadPos == d.readPos {
				// 从磁盘中读取一条message
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s", d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			// 这里表明没有可以读取的数据
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// 当有数据可以进行读取的时候，r 不为空
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			// 更新readPos, readFileNum, nextReadPos, nextReadFileNum的信息
			// 数据读取过后才更新readPos
			d.moveForward()
		// 接收到清空数据的请求
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		// 当有数据写入的时候，并没有直接将数据直接写入磁盘，而是先写入writeChan，然后在这里再写入磁盘
		case dataWrite := <-d.writeChan:
			count++
			// 将数据写入磁盘中，并将操作结果写入writeResponseChan中。
			d.writeResponseChan <- d.writeOne(dataWrite)
		// 定时器响应，此时需要将metaData和w.writeFile进行持久化
		case <-syncTicker.C:
			// 如果没有读取和写入任何消息，则不作处理。
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		// 接受到退出命令
		case <-d.exitChan:
			// 在这里没有保存一下数据相关信息啥的吗？
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop() // 停掉计时器
	d.exitSyncChan <- 1
}
