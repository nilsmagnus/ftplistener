package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"sync"

	"github.com/jlaffaye/ftp"
	nats "github.com/nats-io/go-nats-streaming"
)

type ByDate []*ftp.Entry

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i].Time.After(a[j].Time) }

func (f *ftpEntryForDownload) isEmpty() bool {
	return f.subDir == "" && f.destinationFolder == "" && f.baseDir == ""
}

func main() {

	hostName := flag.String("host", "ftp.ncep.noaa.gov", "Ftp host to ftpConnect to")
	port := flag.String("port", "21", "Ftp port to ftpConnect to")
	baseDir := flag.String("baseDir", "/pub/data/nccf/com/gfs/prod/", "Base dir")
	user := flag.String("user", "anonymous", "ftp user")
	password := flag.String("password", "anything", "ftp password")
	saveFolder := flag.String("destination", "gribfiles", "destination for downloaded files")

	flag.Parse()

	host := fmt.Sprintf("%s:%s", *hostName, *port)

	credentials := map[string]string{
		"user":     *user,
		"password": *password,
		"host":     host,
	}

	conn, connectErr := ftpConnect(credentials)

	if connectErr != nil {
		panic(connectErr)
	}
	//
	folderList, listErr := conn.List(*baseDir)

	if listErr != nil {
		panic(listErr)
	}

	log.Printf("got %d folders from basedir %s\n", len(folderList), *baseDir)

	sort.Sort(ByDate(folderList))

	gfsFolderName, _ := regexp.Compile("gfs.([0-9]{8})")

	downloadItemChannel := make(chan ftpEntryForDownload, 1000)

	wg := sync.WaitGroup{}
	maxConcurrentDownloads := make(chan int, 16)

	onDone, sc := postToNatsFunc("nats://pi.hole:4222")

	if sc != nil {
		defer sc.Close()
	}

	go func() {
		for {
			select {
			case entry := <-downloadItemChannel:
				wg.Add(1)
				go func() {
					err := downloadSingle(credentials, entry, maxConcurrentDownloads, onDone)
					if err != nil {
						log.Println("Failed to download entry", "entry", entry.entry.Name, "date", entry.entry.Time, "error", err.Error())
						downloadItemChannel <- entry
					}
					wg.Done()
				}()
			}
		}

	}()
	for _, ftpFolder := range folderList {
		if folderIsRelevant(ftpFolder, gfsFolderName) {
			log.Println("hit ftpFolder ", "foldername", ftpFolder.Name)
			for _, subFolderName := range []string{"00", "06", "12", "18"} {
				aboluteFolder := fmt.Sprintf("%s/%s", ftpFolder.Name, subFolderName)
				if gribFiles, err := listFiles(credentials, *baseDir, aboluteFolder); err == nil {
					sort.Sort(ByDate(gribFiles))
					log.Printf("Found %d files in subfolder %s\n", len(gribFiles), aboluteFolder)
					putAllEntriesInFolderOnChannel(downloadItemChannel, *baseDir, aboluteFolder, gribFiles, *saveFolder)
				} else {
					log.Println("Error listing files in folder ", "folder", ftpFolder.Name)
				}

			}
		} else {
			//	log.Printf("folder is not relevant: %v\n", ftpFolder.Name)
		}
	}

	fmt.Println("wait syncgroup")
	wg.Wait()
	fmt.Println("Syncgroup is done.")

}

func putAllEntriesInFolderOnChannel(downloadChannel chan<- ftpEntryForDownload, baseDir, subDir string, entries []*ftp.Entry, destinationFolder string) {
	for _, fileEntry := range entries {
		stat, err := os.Stat(filePath(destinationFolder, fileEntry, subDir))

		if os.IsNotExist(err) { // if file does not exist
			downloadChannel <- ftpEntryForDownload{
				baseDir:           baseDir,
				subDir:            subDir,
				entry:             fileEntry,
				destinationFolder: destinationFolder,
			}
		} else if stat != nil && stat.Size() != int64(fileEntry.Size) { // if filesize is different from existing file
			log.Println("Deleting incomplete entry ", "entry", filePath(destinationFolder, fileEntry, subDir))
			os.Remove(stat.Name())
			downloadChannel <- ftpEntryForDownload{
				baseDir:           baseDir,
				subDir:            subDir,
				entry:             fileEntry,
				destinationFolder: destinationFolder,
			}
		} else {
			log.Println("Skipping existing entry", "entry", filePath(destinationFolder, fileEntry, subDir))
		}
	}
}

type ftpEntryForDownload struct {
	baseDir           string
	subDir            string
	entry             *ftp.Entry
	destinationFolder string
}

func downloadSingle(credentials map[string]string, downloadItem ftpEntryForDownload, maxConcurrentDownloads chan int, onDone func(filename string)) error {
	maxConcurrentDownloads <- 0

	defer func() {
		filename := filePath(downloadItem.destinationFolder, downloadItem.entry, downloadItem.subDir)
		onDone(filename)
		log.Println("Done downloading ", "file", filename)
		<-maxConcurrentDownloads
	}()
	log.Println("Downloading ", "file", filePath(downloadItem.destinationFolder, downloadItem.entry, downloadItem.subDir))

	os.MkdirAll(fileFolder(downloadItem.destinationFolder, downloadItem.subDir), 0777)

	conn, err := ftpConnect(credentials)
	if err != nil {
		return err
	}
	defer conn.Logout()

	conn.ChangeDir(downloadItem.baseDir + "/" + downloadItem.subDir)

	response, err := conn.Retr(downloadItem.entry.Name)
	if err != nil {
		return err
	}
	defer response.Close()

	fileName := filePath(downloadItem.destinationFolder, downloadItem.entry, downloadItem.subDir)

	file, ferr := os.Create(fileName)
	if ferr != nil {
		return ferr
	}

	defer file.Close()
	_, writeErr := io.Copy(file, response) // todo inspect content if you want to here

	if writeErr != nil {
		return writeErr
	}
	return nil
}

func fileFolder(folderName, subdir string) string {
	return fmt.Sprintf("%s/%s/", folderName, subdir)
}
func filePath(folderName string, entry *ftp.Entry, subdir string) string {
	return fmt.Sprintf("%s%s", fileFolder(folderName, subdir), entry.Name)
}

func listFiles(credentials map[string]string, baseDir string, subDir string) ([]*ftp.Entry, error) {
	conn, conErr := ftpConnect(credentials)
	if conErr != nil {
		return nil, conErr
	}

	list, err := conn.List(baseDir + "/" + subDir)

	if err != nil {
		return nil, err
	}

	gfsFileName, _ := regexp.Compile("gfs.t([0-9]{2})z.pgrb2.1p00.f([0-9]{3})")

	relevantList := make([]*ftp.Entry, 0)
	for _, e := range list {
		if e.Type == ftp.EntryTypeFile &&
			gfsFileName.MatchString(e.Name) &&
			!strings.Contains(e.Name, "idx") {
			relevantList = append(relevantList, e)
		}
	}

	return relevantList, nil

}

func folderIsRelevant(l *ftp.Entry, gfsFolderName *regexp.Regexp) bool {
	return l.Type == ftp.EntryTypeFolder && gfsFolderName.MatchString(l.Name)
}
func ftpConnect(credentials map[string]string) (*ftp.ServerConn, error) {
	conn, err := ftp.DialTimeout(credentials["host"], 15*time.Second)
	if err != nil {
		return nil, err
	}
	loginErr := conn.Login(credentials["user"], credentials["password"])
	if loginErr != nil {
		return nil, loginErr
	}
	return conn, nil
}

func postToNatsFunc(natsUrl string) (func(filename string), nats.Conn) {
	sc, connectError := nats.Connect("test-cluster", "ftplistener", nats.NatsURL(natsUrl))

	if connectError != nil {
		log.Println("Nats unavailable", connectError.Error())
		return func(name string) {
			log.Println("Error connecting to nats, ", connectError.Error(), " not publishing events", name)
		}, nil
	}

	log.Println("Connected to nats on ", natsUrl)

	return func(name string) {
		if publishError := sc.Publish("leia.noaa.files", []byte(name)); publishError != nil {
			log.Println(publishError.Error())
		}
	}, sc
}
