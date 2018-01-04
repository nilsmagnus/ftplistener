package main

import (
	"github.com/jlaffaye/ftp"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"os"
	"io"
	"sort"
	"sync"
)

type ByDate []*ftp.Entry

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i].Time.After(a[j].Time) }

func main() {

	hostName := flag.String("host", "ftp.ncep.noaa.gov", "Ftp host to connect to")
	port := flag.String("port", "21", "Ftp port to connect to")
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

	conn, connectErr := connect(credentials)

	if connectErr != nil {
		panic(connectErr)
	}

	folderList, listErr := conn.List(*baseDir)

	if listErr != nil {
		panic(listErr)
	}

	sort.Sort(ByDate(folderList))

	gfsFolderName, _ := regexp.Compile("gfs.([0-9]{8})")

	for _, ftpFolder := range folderList {
		if folderIsRelevant(ftpFolder, gfsFolderName) {
			fmt.Printf("->\thit ftpFolder %s\n", ftpFolder.Name)
			if gribFiles, err := listFiles(credentials, *baseDir, ftpFolder.Name); err == nil {
				sort.Sort(ByDate(gribFiles))
				downloadAll(credentials, *baseDir, ftpFolder.Name, gribFiles, *saveFolder)
			} else {
				fmt.Printf("Error listing files in folder [%s] \n", ftpFolder.Name)
			}
		}
	}

}
func downloadAll(credentials map[string]string, baseDir, subDir string, entries []*ftp.Entry, destinationFolder string) {
	var wg sync.WaitGroup
	for _, fileEntry := range entries {
		stat, err := os.Stat(filePath(destinationFolder, fileEntry, subDir))

		if os.IsNotExist(err) { // if file does not exist
			wg.Add(1)
			go downloadSingle(credentials, baseDir, subDir, fileEntry, destinationFolder, &wg)
		} else if stat != nil && stat.Size() != int64(fileEntry.Size) { // if filesize is different from existing file
			fmt.Printf("Deleting incomplete entry %s\n", filePath(destinationFolder, fileEntry, subDir))
			os.Remove(stat.Name())
			wg.Add(1)
			go downloadSingle(credentials, baseDir, subDir, fileEntry, destinationFolder, &wg)
		} else {
			fmt.Printf("Skipping existing entry %s\n", filePath(destinationFolder, fileEntry, subDir))
		}
	}
	wg.Wait()
}

func downloadSingle(credentials map[string]string, baseDir, subDir string, entry *ftp.Entry, destinationFolder string, wg *sync.WaitGroup) error {
	defer wg.Done()

	fmt.Printf("Downloading %s\n", filePath(destinationFolder, entry, subDir))

	os.MkdirAll(fileFolder(destinationFolder,  subDir), 0777)

	conn, err := connect(credentials)
	if err != nil {
		return err
	}
	defer conn.Logout()

	conn.ChangeDir(baseDir + "/" + subDir)

	response, err := conn.Retr(entry.Name)
	if err != nil {
		return err
	}
	defer response.Close()

	fileName := filePath(destinationFolder, entry, subDir)

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

func fileFolder(folderName , subdir string ) (string) {
	return fmt.Sprintf("%s/%s/", folderName, subdir)
}
func filePath(folderName string, entry *ftp.Entry, subdir string) string {
	return fmt.Sprintf("%s%s", fileFolder(folderName, subdir), entry.Name)
}

func listFiles(credentials map[string]string, baseDir string, subDir string) ([]*ftp.Entry, error) {
	conn, conErr := connect(credentials)
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
func connect(credentials map[string]string) (*ftp.ServerConn, error) {
	conn, err := ftp.Dial(credentials["host"])
	if err != nil {
		return nil, err
	}
	loginErr := conn.Login(credentials["user"], credentials["password"])
	if loginErr != nil {
		return nil, loginErr
	}
	return conn, nil
}
