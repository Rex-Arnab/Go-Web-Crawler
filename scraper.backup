package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

const (
	stateFileName   = "crawl_state.json"
	downloadBaseDir = "downloaded_pages"
	numWorkers      = 10                      // Concurrency level
	requestDelay    = 2000 * time.Millisecond // Optional delay between requests per worker
)

// LinkInfo stores details about a discovered URL
type LinkInfo struct {
	URL          string    `json:"url"`             // The absolute URL
	Downloaded   bool      `json:"downloaded"`      // Has the HTML been downloaded?
	LocalPath    string    `json:"localPath"`       // Path to the saved HTML file
	ContentType  string    `json:"contentType"`     // Content-Type detected during download attempt
	StatusCode   int       `json:"statusCode"`      // HTTP status code during download attempt
	Error        string    `json:"error,omitempty"` // Any error encountered during download/processing
	DiscoveredAt time.Time `json:"discoveredAt"`    // When the link was first found
	ProcessedAt  time.Time `json:"processedAt"`     // When the download/processing attempt finished
}

// CrawlState represents the overall state of the crawl, saved to JSON
type CrawlState struct {
	StartURL    string               `json:"startUrl"`
	BaseHost    string               `json:"baseHost"`
	LastUpdated time.Time            `json:"lastUpdated"`
	Links       map[string]*LinkInfo `json:"links"` // Map URL string to its LinkInfo
}

// StateManager handles concurrent access to the CrawlState
type StateManager struct {
	mu    sync.RWMutex
	state *CrawlState
}

// NewStateManager creates a StateManager, loading existing state if possible
func NewStateManager(startURL *url.URL) *StateManager {
	sm := &StateManager{
		state: &CrawlState{
			StartURL: startURL.String(),
			BaseHost: startURL.Host,
			Links:    make(map[string]*LinkInfo),
		},
	}
	sm.loadState() // Attempt to load previous state
	return sm
}

// loadState reads the state file from disk
func (sm *StateManager) loadState() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := os.ReadFile(stateFileName)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("State file '%s' not found, starting fresh.", stateFileName)
			return // No existing state
		}
		log.Printf("Error reading state file '%s': %v. Starting fresh.", stateFileName, err)
		return
	}

	err = json.Unmarshal(data, sm.state)
	if err != nil {
		log.Printf("Error unmarshalling state file '%s': %v. Starting fresh.", stateFileName, err)
		// Reset to a clean state if unmarshalling fails
		sm.state.Links = make(map[string]*LinkInfo)
		return
	}

	log.Printf("Loaded %d URLs from state file '%s'.", len(sm.state.Links), stateFileName)
	// Optional: Validate loaded state (e.g., check if StartURL matches)
	if sm.state.BaseHost == "" && sm.state.StartURL != "" { // Handle older state files potentially missing BaseHost
		LbaseURL, err := url.Parse(sm.state.StartURL)
		if err == nil {
			sm.state.BaseHost = LbaseURL.Host
		}
	}
}

// saveState writes the current state to disk
func (sm *StateManager) saveState() {
	sm.mu.RLock() // Use RLock for reading state data before marshalling
	sm.state.LastUpdated = time.Now()
	data, err := json.MarshalIndent(sm.state, "", "  ")
	sm.mu.RUnlock()

	if err != nil {
		log.Printf("Error marshalling state: %v", err)
		return
	}

	err = os.WriteFile(stateFileName, data, 0644)
	if err != nil {
		log.Printf("Error writing state file '%s': %v", stateFileName, err)
	} else {
		log.Printf("Saved state with %d URLs to '%s'.", len(sm.state.Links), stateFileName)
	}
}

// AddLink adds a new URL to the state if it's not already known. Returns true if added.
func (sm *StateManager) AddLink(urlStr string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.state.Links[urlStr]; exists {
		return false // Already known
	}

	sm.state.Links[urlStr] = &LinkInfo{
		URL:          urlStr,
		Downloaded:   false,
		DiscoveredAt: time.Now(),
	}
	log.Printf("Discovered new link: %s", urlStr)
	return true
}

// NeedsProcessing checks if a URL is known but not yet downloaded/processed.
func (sm *StateManager) NeedsProcessing(urlStr string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	info, exists := sm.state.Links[urlStr]
	return exists && !info.Downloaded && info.Error == "" // Process if known, not downloaded, and no permanent error
}

// UpdateLink updates the information for a given URL (e.g., after download attempt)
func (sm *StateManager) UpdateLink(info *LinkInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	info.ProcessedAt = time.Now()
	sm.state.Links[info.URL] = info // Overwrite existing entry with updated info
}

// GetLinkInfo retrieves the LinkInfo for a URL. Returns info and true if exists.
func (sm *StateManager) GetLinkInfo(urlStr string) (*LinkInfo, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	info, exists := sm.state.Links[urlStr]
	// Return a copy to prevent race conditions if the caller modifies it
	if !exists {
		return nil, false
	}
	infoCopy := *info // Create a shallow copy
	return &infoCopy, true
}

// GetTotalLinks returns the total number of unique links discovered.
func (sm *StateManager) GetTotalLinks() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.state.Links)
}

// GetDownloadedCount returns the number of links successfully downloaded.
func (sm *StateManager) GetDownloadedCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	count := 0
	for _, info := range sm.state.Links {
		if info.Downloaded {
			count++
		}
	}
	return count
}

// --- Main Scraping Logic ---

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <start_url>\n", os.Args[0])
		os.Exit(1)
	}
	startURLArg := os.Args[1]

	// Validate and parse the starting URL
	baseURL, err := url.Parse(startURLArg)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		log.Fatalf("Invalid starting URL: %v", err)
	}
	// Ensure base URL doesn't have path/query/fragment for domain checking
	baseDomainURL := &url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
	}
	normalizedStartURL := baseURL.String() // Use the potentially redirected URL later if needed

	fmt.Printf("Starting crawl from: %s\n", baseURL.String())
	fmt.Printf("Target Domain Host: %s\n", baseDomainURL.Host)
	fmt.Printf("Saving state to: %s\n", stateFileName)
	fmt.Printf("Downloading pages to: %s/\n", downloadBaseDir)

	// Create download directory if it doesn't exist
	err = os.MkdirAll(downloadBaseDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create download directory '%s': %v", downloadBaseDir, err)
	}

	// Initialize State Manager (loads existing state)
	stateManager := NewStateManager(baseURL)

	// Use channels and WaitGroup for concurrency
	queue := make(chan string, 1000) // Buffered channel for URLs to process
	var wg sync.WaitGroup

	// Add the starting URL to the state if it's new
	// And add it to the queue if it needs processing
	initialURLAdded := stateManager.AddLink(normalizedStartURL)
	if initialURLAdded || stateManager.NeedsProcessing(normalizedStartURL) {
		wg.Add(1)
		queue <- normalizedStartURL
	} else {
		// If start URL already downloaded, seed queue with its known unprocessed links
		log.Printf("Start URL %s already processed. Seeding queue with its known unprocessed links.", normalizedStartURL)
		stateManager.mu.RLock() // Need to read the state
		for urlStr, info := range stateManager.state.Links {
			if info.Error == "" && !info.Downloaded {
				// Check if internal (use BaseHost from loaded state)
				linkURL, err := url.Parse(urlStr)
				if err == nil && isInternal(linkURL, &url.URL{Scheme: "http", Host: stateManager.state.BaseHost}) { // Assume http if scheme missing, Host matters most
					wg.Add(1)
					queue <- urlStr
				}
			}
		}
		stateManager.mu.RUnlock()
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		go worker(queue, &wg, stateManager, baseDomainURL)
	}

	// Wait for all crawling tasks to complete
	wg.Wait()
	close(queue) // Close queue once all work is done

	// Save the final state
	stateManager.saveState()

	// Print summary
	fmt.Println("\n--- Crawl Summary ---")
	total := stateManager.GetTotalLinks()
	downloaded := stateManager.GetDownloadedCount()
	fmt.Printf("Total unique internal URLs discovered: %d\n", total)
	fmt.Printf("HTML pages successfully downloaded:   %d\n", downloaded)
	fmt.Printf("State saved to: %s\n", stateFileName)
	fmt.Printf("Pages downloaded to: %s/\n", downloadBaseDir)
}

// worker processes URLs from the queue
func worker(queue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL) {
	for urlStr := range queue {
		// Signal that this task is done when we exit this iteration
		defer wg.Done()

		// Optional delay to be polite to the server
		if requestDelay > 0 {
			time.Sleep(requestDelay)
		}

		// Check again if it still needs processing, another worker might have finished it
		if sm.NeedsProcessing(urlStr) {
			processPage(urlStr, queue, wg, sm, baseDomainURL)
		}
	}
}

// processPage downloads a page, saves it, updates state, and finds new links
func processPage(urlStr string, queue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL) {
	fmt.Println("Processing:", urlStr)

	// Get the current LinkInfo (it should exist if we're here)
	currentInfo, exists := sm.GetLinkInfo(urlStr)
	if !exists {
		log.Printf("Error: URL %s not found in state during processing.", urlStr)
		return // Should not happen if logic is correct
	}

	// --- Download Step ---
	localPath, contentType, statusCode, downloadErr := downloadHTML(urlStr, baseDomainURL.Host)

	// Update LinkInfo with download results
	currentInfo.StatusCode = statusCode
	currentInfo.ContentType = contentType
	currentInfo.Downloaded = (downloadErr == nil && statusCode == http.StatusOK && strings.HasPrefix(contentType, "text/html"))
	currentInfo.LocalPath = localPath // Store path even if download failed (might be empty)

	if downloadErr != nil {
		log.Printf("Error downloading %s: %v", urlStr, downloadErr)
		currentInfo.Error = downloadErr.Error()
		sm.UpdateLink(currentInfo) // Save error state
		return                     // Stop processing this page if download failed
	}

	if statusCode != http.StatusOK {
		log.Printf("Non-OK status %d for %s", statusCode, urlStr)
		currentInfo.Error = fmt.Sprintf("HTTP status %d", statusCode)
		sm.UpdateLink(currentInfo) // Save error state
		// Decide if we want to parse non-200 pages (e.g., 404 might have links)
		// For now, we stop if not OK.
		return
	}

	if !strings.HasPrefix(contentType, "text/html") {
		log.Printf("Skipping link extraction for non-HTML content (%s) at %s", contentType, urlStr)
		currentInfo.Downloaded = true // Mark as 'downloaded' but not HTML, won't parse
		currentInfo.Error = ""        // Clear any previous errors if download was successful now
		sm.UpdateLink(currentInfo)
		return
	}

	// Download successful and it's HTML
	currentInfo.Downloaded = true
	currentInfo.Error = "" // Clear previous errors
	sm.UpdateLink(currentInfo)

	// --- Link Extraction Step (only if download was successful HTML) ---
	file, err := os.Open(localPath)
	if err != nil {
		log.Printf("Error opening downloaded file %s for parsing: %v", localPath, err)
		// Update state with this new error? Maybe not, download succeeded but file vanished?
		return
	}
	defer file.Close()

	// We need the URL the *content* was actually served from for resolving relative links.
	// For simplicity here, we'll use the original urlStr's parsed version.
	// A more robust solution would get this from the http response (`resp.Request.URL`).
	// We'll pass the parsed version of urlStr for resolving.
	pageURL, parseErr := url.Parse(urlStr)
	if parseErr != nil {
		log.Printf("Error parsing URL %s for link resolution: %v", urlStr, parseErr)
		return // Cannot resolve relative links without a valid base
	}

	links := findLinks(file, pageURL) // Pass the file reader

	// Process found links
	for _, link := range links {
		absoluteURL := resolveURL(pageURL, link) // Resolve using the page's URL as base
		if absoluteURL == nil {
			continue // Skip invalid or non-http(s) links
		}

		// Normalize URL (remove fragment, ensure consistent string representation)
		absoluteURL.Fragment = ""
		normalizedLinkStr := absoluteURL.String()

		// Check if it's internal
		if isInternal(absoluteURL, baseDomainURL) {
			// Try adding the link to the state. If added (it was new):
			if sm.AddLink(normalizedLinkStr) {
				// Add to the queue for processing with timeout to prevent deadlock
				wg.Add(1) // Increment WaitGroup counter *before* sending to queue
				go func(u string) {
					select {
					case queue <- u:
						// Successfully queued
					case <-time.After(5 * time.Second):
						log.Printf("Timeout adding URL %s to queue - may be full", u)
						wg.Done() // Release the WaitGroup if we couldn't queue
					}
				}(normalizedLinkStr)
			}
			// Note: We don't need to explicitly queue links that were already known but not downloaded,
			// because the initial seeding of the queue (or the main loop finding them) should handle it.
			// The worker's check `NeedsProcessing` ensures it gets processed eventually if required.
		}
	}
}

// downloadHTML fetches the content of a URL and saves it locally.
// Returns the local path, content type, status code, and error.
func downloadHTML(urlStr string, baseHost string) (string, string, int, error) {
	client := &http.Client{
		Timeout: 15 * time.Second, // Add a timeout
	}
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", "", 0, fmt.Errorf("creating request failed: %w", err)
	}
	req.Header.Set("User-Agent", "GoInternalLinkScraper/1.1 (Offline Cache Bot)")
	// Optional: Add Accept header
	// req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", 0, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	// Get final URL after potential redirects
	finalURL := resp.Request.URL
	finalURLStr := finalURL.String() // Use this for logging/state if different from input urlStr

	if finalURL.Host != baseHost {
		// It redirected outside the target domain, treat as an error for this crawler's purpose
		log.Printf("Redirected outside target domain: %s -> %s", urlStr, finalURLStr)
		return "", resp.Header.Get("Content-Type"), resp.StatusCode, fmt.Errorf("redirected to external domain %s", finalURL.Host)
	}

	contentType := resp.Header.Get("Content-Type")
	statusCode := resp.StatusCode

	// Generate local file path
	localPath := getFilePath(finalURL) // Use the final URL for path generation
	if localPath == "" {
		return "", contentType, statusCode, fmt.Errorf("could not generate valid file path for %s", finalURLStr)
	}

	// Create directory structure if needed
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", contentType, statusCode, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create the destination file
	file, err := os.Create(localPath)
	if err != nil {
		return localPath, contentType, statusCode, fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	// Save the response body to the file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		// Attempt to remove partially written file on copy error
		os.Remove(localPath)
		return localPath, contentType, statusCode, fmt.Errorf("failed to write file %s: %w", localPath, err)
	}

	log.Printf("Downloaded: %s -> %s (Status: %d, Type: %s)", finalURLStr, localPath, statusCode, contentType)

	// Return the path, content type, status code, and nil error for success
	return localPath, contentType, statusCode, nil
}

// getFilePath generates a local file path based on the URL structure
func getFilePath(u *url.URL) string {
	// Sanitize host to be used as a directory name
	hostDir := strings.ReplaceAll(u.Host, ":", "_") // Replace port separator if present

	// Start with base directory and sanitized host
	path := filepath.Join(downloadBaseDir, hostDir)

	// Add path components
	if u.Path != "" && u.Path != "/" {
		// Clean the path and split into components
		cleanedPath := filepath.Clean(u.Path)
		// Prepend / to ensure Join treats it as absolute relative to 'path' var
		path = filepath.Join(path, cleanedPath)
	}

	// If the path ends in a directory or is empty, assume index.html
	if strings.HasSuffix(u.Path, "/") || u.Path == "" || u.Path == "/" {
		path = filepath.Join(path, "index.html")
	} else {
		// Ensure it has an .html extension if it doesn't have one
		if filepath.Ext(path) == "" {
			path += ".html"
		}
		// Could also check if existing extension makes sense, e.g. .php -> .html?
		// For simplicity, we'll just ensure .html if no extension present.
	}

	// Basic sanitization: Replace common problematic characters in file/dir names
	// This is rudimentary, more robust sanitization might be needed for edge cases.
	path = strings.ReplaceAll(path, "?", "_query_")
	path = strings.ReplaceAll(path, "*", "_star_")
	// Add more replacements as needed based on your OS filesystem constraints

	// Check length limits? (less common issue now, but possible)

	return path
}

// findLinks extracts all href links from an HTML document (Reader)
func findLinks(body io.Reader, pageURL *url.URL) []string {
	var links []string
	tokenizer := html.NewTokenizer(body)

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			err := tokenizer.Err()
			if err != io.EOF {
				log.Printf("HTML parsing error on page %s: %v", pageURL.String(), err)
			}
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						links = append(links, strings.TrimSpace(attr.Val))
						break
					}
				}
			}
			// Optional: Find links in other tags like <link rel="canonical" href="...">
			// if token.Data == "link" { ... check rel and href ... }
		}
	}
}

// resolveURL makes a relative URL absolute
func resolveURL(base *url.URL, relative string) *url.URL {
	if relative == "" || strings.HasPrefix(relative, "#") || strings.HasPrefix(strings.ToLower(relative), "mailto:") || strings.HasPrefix(strings.ToLower(relative), "tel:") || strings.HasPrefix(strings.ToLower(relative), "javascript:") {
		return nil // Ignore fragments, mailto, tel, javascript links
	}

	relURL, err := url.Parse(relative)
	if err != nil {
		// log.Printf("Warn: Could not parse relative link '%s' on page %s: %v", relative, base.String(), err)
		return nil
	}

	absoluteURL := base.ResolveReference(relURL)

	if absoluteURL.Scheme != "http" && absoluteURL.Scheme != "https" {
		return nil // Only keep http/https links
	}

	return absoluteURL
}

// isInternal checks if a given URL belongs to the same host as the base domain URL
func isInternal(linkURL *url.URL, baseDomainURL *url.URL) bool {
	return linkURL.Host == baseDomainURL.Host
}
