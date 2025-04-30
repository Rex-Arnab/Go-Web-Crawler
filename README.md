# Go Web Crawler

A concurrent web crawler written in Go with terminal UI, state persistence, and intelligent resource handling.

![Demo](demo.webm)

## Features

- **Concurrent crawling** with configurable worker pools
- **Terminal UI** showing real-time progress and statistics
- **State persistence** with automatic saving/loading
- **HTML parsing** with link discovery
- **Asset downloading** (CSS, JS, images, etc.)
- **Redirect handling** with tracking
- **Cloudflare challenge detection**
- **Rate limiting** to avoid overwhelming servers
- **Cross-platform** - runs anywhere Go runs

## Installation

1. Ensure you have Go installed (version 1.20+ recommended)
2. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/go-web-crawler.git
   cd go-web-crawler
   ```
3. Build the project:
   ```bash
   go build
   ```

## Usage

Basic crawling:
```bash
./go-web-crawler https://example.com
```

The crawler will:
1. Start from the given URL
2. Discover and download linked pages
3. Download referenced assets
4. Save everything to `downloaded_pages/`

### Controls

While running:
- `q` or `Ctrl+C` to quit
- TUI shows:
  - Active tasks
  - Queue sizes
  - Statistics (discovered, downloaded, errors)
  - Recent events

## Configuration

You can adjust these constants in `scraper.go`:

```go
const (
    numTotalWorkers      = 100    // Total concurrent workers
    minDownloadWorkers   = 50     // Minimum download workers
    requestDelay         = 250ms  // Delay between requests
    discoveryDelay       = 50ms   // Delay between discoveries
    saveStateInterval    = 15s    // How often to save state
)
```

## Demo

See the crawler in action:

[![Demo Video](demo.webm)](demo.webm)

## License

MIT
