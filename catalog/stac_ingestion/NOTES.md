## Implementation details


### arturo-stac-api required changes

* Enable CORS 
```
from starlette.middleware.cors import CORSMiddleware
...
app.add_middleware(CORSMiddleware, allow_origins=['*'])
```

* Enable `ContextExtension()`


### stac-browser required changes

* Remove use of `const { hash } = url.parse(uri);`