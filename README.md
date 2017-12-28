get latest grib2 files from nooa via anonymous ftp (ftp.ncep.noaa.gov)

# prereqs

* go 1.8+ installed
* dep installed

# build & run


    dep ensure
    go build
    ./ftplistener
    

* will get dependencies(ftp-client), build the application and start downloading all current gribfiles from nooa. 
* checks for complete duplicates before downloading
* deletes existing incomplete downloads

# usage
    Usage of ./ftplistener:
      -baseDir string
        	Base dir (default "/pub/data/nccf/com/gfs/prod")
      -destination string
        	destination for downloaded files (default "gribfiles")
      -host string
        	Ftp host to connect to (default "ftp.ncep.noaa.gov")
      -password string
        	ftp password (default "anything")
      -port string
        	Ftp port to connect to (default "21")
      -user string
        	ftp user (default "anonymous")


# gotchas

* downloads only 1p00 files, change the source if you need something else
* will only verify size of files, not checksum
* all files for 1 day is about 4GB in size

    
