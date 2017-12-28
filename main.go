package main

import (
	"github.com/jlaffaye/ftp"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"os"
	"time"
	"io"
)

func main() {

	hostName := flag.String("host", "ftp.ncep.noaa.gov", "Ftp host to connect to")
	port := flag.String("port", "21", "Ftp port to connect to")
	baseDir := flag.String("baseDir", "/pub/data/nccf/com/gfs/prod", "Base dir")
	user := flag.String("user", "anonymous", "ftp user")
	password := flag.String("password", "anything", "ftp password")
	saveFolder := flag.String("destination", "gribfiles", "destination for downloaded files")

	flag.Parse()

	host := fmt.Sprintf("%s:%s", *hostName, *port)

	conn, connectErr := connect(host, *user, *password)

	if connectErr != nil {
		panic(connectErr)
	}

	list, listErr := conn.List(*baseDir)

	if listErr != nil {
		panic(listErr)
	}

	gfsFolderName, _ := regexp.Compile("gfs.([0-9]{8})")

	for _, entry := range list {
		if folderIsRelevant(entry, gfsFolderName) {
			fmt.Printf("->\thit folder %s\n", entry.Name)
			if files, err := listFiles(conn, *baseDir, entry.Name); err == nil {
				downloadAll(conn, files, *saveFolder)
			}
		}
	}

}
func downloadAll(conn *ftp.ServerConn, entries []*ftp.Entry, destinationFolder string) {
	for _, fileEntry := range entries {
		stat, err := os.Stat(filePath(destinationFolder, fileEntry))

		if os.IsNotExist(err) { // if file does not exist
			downloadSingle(conn, fileEntry, destinationFolder)
		} else if stat != nil && stat.Size() != int64(fileEntry.Size) { // if filesize is different from existing file
			fmt.Printf("Deleting incomplete entry %s\n",  filePath(destinationFolder, fileEntry))
			os.Remove(stat.Name())
			downloadSingle(conn, fileEntry, destinationFolder)
		} else {
			fmt.Printf("Skipping existing entry %s\n", filePath(destinationFolder, fileEntry))
		}
	}
}

func downloadSingle(conn *ftp.ServerConn, entry *ftp.Entry, destinationFolder string) error {
	fmt.Printf("Downloading %s\n", filePath(destinationFolder, entry))

	os.MkdirAll(fileFolder(destinationFolder, entry.Time), 0777)

	response, err := conn.Retr(entry.Name)
	if err != nil {
		return err
	}
	defer response.Close()

	fileName := filePath(destinationFolder, entry)

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

func fileFolder(folderName string, entryTime time.Time) (string) {
	return fmt.Sprintf("%s/%d%2d%2d%2d/", folderName, entryTime.Year(), entryTime.Month(), entryTime.Day(), entryTime.Hour())
}
func filePath(folderName string, entry *ftp.Entry) string {

	return fmt.Sprintf("%s%s", fileFolder(folderName, entry.Time), entry.Name)
}
func listFiles(conn *ftp.ServerConn, baseDir string, subDir string) ([]*ftp.Entry, error) {

	dirErr := conn.ChangeDir(baseDir + "/" + subDir)
	if dirErr != nil {
		return nil, dirErr
	}

	list, err := conn.List("")

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
func connect(host, user, password string) (*ftp.ServerConn, error) {
	conn, err := ftp.Dial(host)
	if err != nil {
		return nil, err
	}
	loginErr := conn.Login(user, password)
	if loginErr != nil {
		return nil, loginErr
	}
	return conn, nil
}
