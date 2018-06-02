package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"sync"

	"github.com/jlaffaye/ftp"
	"go.uber.org/zap"
)

type ByDate []*ftp.Entry

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i].Time.After(a[j].Time) }

func (f *ftpEntryForDownload) isEmpty() bool {
	return f.subDir == "" && f.destinationFolder == "" && f.baseDir == ""
}

func main() {

	logger, _ := zap.NewDevelopment()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()

	hostName := flag.String("host", "ftp.ncep.noaa.gov", "Ftp host to ftpConnect to")
	port := flag.String("port", "21", "Ftp port to ftpConnect to")
	baseDir := flag.String("baseDir", "/pub/data/nccf/com/gfs/prod", "Base dir")
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

	folderList, listErr := conn.List(*baseDir)

	if listErr != nil {
		panic(listErr)
	}

	sort.Sort(ByDate(folderList))

	gfsFolderName, _ := regexp.Compile("gfs.([0-9]{8})")

	downloadItemChannel := make(chan ftpEntryForDownload, 1000)

	wg := sync.WaitGroup{}
	maxConcurrentDownloads := make(chan int, 16)

	go func() {
		for {
			select {
			case entry := <-downloadItemChannel:
				wg.Add(1)
				go func() {
					err := downloadSingle(credentials, entry, maxConcurrentDownloads, sugar)
					if err != nil {
						sugar.Warnw("Failed to download entry", "entry", entry.entry.Name, "date", entry.entry.Time)
						// TODO log something here
						downloadItemChannel <- entry
					}
					wg.Done()
				}()
			}
		}

	}()
	for _, ftpFolder := range folderList {
		if folderIsRelevant(ftpFolder, gfsFolderName) {
			sugar.Infow("hit ftpFolder ", "foldername", ftpFolder.Name)
			if gribFiles, err := listFiles(credentials, *baseDir, ftpFolder.Name); err == nil {
				sort.Sort(ByDate(gribFiles))
				putAllEntriesInFolderOnChannel(downloadItemChannel, *baseDir, ftpFolder.Name, gribFiles, *saveFolder, sugar)
			} else {
				sugar.Errorw("Error listing files in folder ", "folder", ftpFolder.Name)
			}
		}
	}

	fmt.Println("wait syncgroup")
	wg.Wait()
	fmt.Println("Syncgroup is done.")

}
func putAllEntriesInFolderOnChannel(downloadChannel chan<- ftpEntryForDownload, baseDir, subDir string, entries []*ftp.Entry, destinationFolder string, sugar *zap.SugaredLogger) {
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
			sugar.Warnw("Deleting incomplete entry ", "entry", filePath(destinationFolder, fileEntry, subDir))
			os.Remove(stat.Name())
			downloadChannel <- ftpEntryForDownload{
				baseDir:           baseDir,
				subDir:            subDir,
				entry:             fileEntry,
				destinationFolder: destinationFolder,
			}
		} else {
			sugar.Infow("Skipping existing entry", "entry", filePath(destinationFolder, fileEntry, subDir))
		}
	}
}

type ftpEntryForDownload struct {
	baseDir           string
	subDir            string
	entry             *ftp.Entry
	destinationFolder string
}

func downloadSingle(credentials map[string]string, downloadItem ftpEntryForDownload, maxConcurrentDownloads chan int, sugar *zap.SugaredLogger) error {
	maxConcurrentDownloads <- 0

	defer func() {
		sugar.Infow("Done downloading ", "file", filePath(downloadItem.destinationFolder, downloadItem.entry, downloadItem.subDir))
		<-maxConcurrentDownloads
	}()
	sugar.Infow("Downloading ", "file", filePath(downloadItem.destinationFolder, downloadItem.entry, downloadItem.subDir))

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

	gfsFileName, _ := regexp.Compile("gfs.t([0-9]{2})z.pgrb2.1p00.f([0-9]{3})") // TODO parameterize this pattern?

	relevantList := make([]*ftp.Entry, 0)
	for _, e := range list {
		if e.Type == ftp.EntryTypeFile && gfsFileName.MatchString(e.Name) && !strings.Contains(e.Name, "idx") {
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
