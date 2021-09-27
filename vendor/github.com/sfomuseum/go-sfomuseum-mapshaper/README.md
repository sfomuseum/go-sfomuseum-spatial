# go-sfomuseum-mapshaper

## Important

Work in progress. Documentation to follow.

## Tools

### server

```
$> bin/server
A simple HTTP server to expose the mapserver-cli tool. Currently, only the '-points inner' functionality is exposed.
Usage:
	 /bin/server [options]
	 
Valid options are:	 
  -mapshaper-path string
    	The path to your mapshaper binary. (default "/usr/local/bin/mapshaper")
  -server-uri string
    	A valid aaronland/go-http-server URI. (default "http://localhost:8080")
  -uploads-max-bytes int
    	The maximum allowed size (in bytes) for uploads. (default 1048576)
```

## See also

* https://github.com/mbloch/mapshaper