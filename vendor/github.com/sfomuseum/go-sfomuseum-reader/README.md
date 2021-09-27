# go-sfomuseum-reader

Common methods for reading SFO Museum (Who's On First) documents.

## Examples

_Note that error handling has been removed for the sake of brevity._

### LoadReadCloserFromID

```
import (
	"context"
	"github.com/whosonfirst/go-reader"
	sfom_reader "github.com/sfomuseum/go-sfomuseum-reader"
	"io"
	"os"
)

func main() {

	ctx := context.Backround()
	wof_id := int64(101736545)

	r_uri := "local:///usr/local/data/whosonfirst-data-admin-ca/data"
	r, _ := reader.NewReader(ctx, r_uri)

	fh, _ := sfom_reader.LoadReadCloserFromID(ctx, r, wof_id)
	io.Copy(os.Stdout, fh)
}
```

### LoadBytesFromID

```
import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-reader"
	sfom_reader "github.com/sfomuseum/go-sfomuseum-reader"
)

func main() {

	ctx := context.Backround()
	wof_id := int64(101736545)

	r_uri := "local:///usr/local/data/whosonfirst-data-admin-ca/data"
	r, _ := reader.NewReader(ctx, r_uri)

	body, _ := sfom_reader.LoadReadCloserFromID(ctx, r, wof_id)
	fmt.Printf("%d bytes\n", len(body))
}
```

### LoadFeatureFromID

```
import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-reader"
	sfom_reader "github.com/sfomuseum/go-sfomuseum-reader"	
)

func main() {

	ctx := context.Backround()
	wof_id := int64(101736545)

	r_uri := "local:///usr/local/data/whosonfirst-data-admin-ca/data"
	r, _ := reader.NewReader(ctx, r_uri)

	f, _ := sfom_reader.LoadFeatureFromID(ctx, r, wof_id)
	fmt.Println(f.Name())
}
```

## See also

* https://github.com/whosonfirst/go-reader