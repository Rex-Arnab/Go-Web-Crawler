package main

import (
	"bufio" // Added for buffered reader
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort" // For sorting active jobs display
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"golang.org/x/net/html"
)

const (
	stateFileName        = "crawl_state.json"
	downloadBaseDir      = "downloaded_pages"
	logFileName          = "crawler.log"          // Log file name
	numTotalWorkers      = 100                    // TOTAL number of workers
	minDownloadWorkers   = 50                     // Minimum workers dedicated to downloading assets/non-parsed items
	requestDelay         = 250 * time.Millisecond // Slightly reduced delay
	discoveryDelay       = 50 * time.Millisecond  // Slightly reduced delay
	maxActiveDisplay     = 10                     // Max URLs to show actively processing in TUI
	maxRecentEvents      = 15                     // Max recent events to show in TUI
	saveStateInterval    = 15 * time.Second
	statsUpdateInterval  = 1 * time.Second // How often to update TUI stats/queue
	assetContentTypes    = "*"             // Allow all content types (used for TUI event logging, not strict filtering)
	discoveryQueueBuffer = 5000            // Larger buffer for potentially many discovered HTML links
	downloadQueueBuffer  = 5000            // Larger buffer for potentially many discovered assets
)

// --- LinkInfo, CrawlState, Status Enum ---
type LinkStatus int

const (
	StatusDiscovered LinkStatus = iota
	StatusQueuedDiscovery
	StatusQueuedDownload
	StatusProcessing // Generic processing start
	StatusDownloading
	StatusParsing
	StatusDownloaded
	StatusRedirected
	StatusError
	StatusBlocked // Added for Cloudflare / JS challenges
)

type LinkInfo struct {
	URL          string     `json:"url"`
	Status       LinkStatus `json:"-"` // Transient TUI status
	Downloaded   bool       `json:"downloaded"`
	IsAsset      bool       `json:"isAsset"`     // Hint from discovery, can be overridden by Content-Type
	LocalPath    string     `json:"localPath"`   // File path OR Redirect Target
	ContentType  string     `json:"contentType"` // Actual content type after download
	StatusCode   int        `json:"statusCode"`
	Error        string     `json:"error,omitempty"`
	DiscoveredAt time.Time  `json:"discoveredAt"`
	ProcessedAt  time.Time  `json:"processedAt"`
}

type CrawlState struct {
	StartURL    string               `json:"startUrl"`
	BaseHost    string               `json:"baseHost"`
	LastUpdated time.Time            `json:"lastUpdated"`
	Links       map[string]*LinkInfo `json:"links"`
}

// --- StateManager ---
type StateManager struct {
	mu    sync.RWMutex
	state *CrawlState
}

func NewStateManager(startURL *url.URL) *StateManager {
	sm := &StateManager{
		state: &CrawlState{
			StartURL: startURL.String(),
			BaseHost: startURL.Host,
			Links:    make(map[string]*LinkInfo),
		},
	}
	sm.loadState()
	return sm
}

func (sm *StateManager) loadState() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	data, err := os.ReadFile(stateFileName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "State file '%s' not found, starting fresh.\n", stateFileName)
			return
		}
		fmt.Fprintf(os.Stderr, "Error reading state file '%s': %v. Starting fresh.\n", stateFileName, err)
		return
	}
	err = json.Unmarshal(data, sm.state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshalling state file '%s': %v. Starting fresh.\n", stateFileName, err)
		sm.state.Links = make(map[string]*LinkInfo)
		return
	}
	fmt.Fprintf(os.Stderr, "Loaded %d URLs from state file '%s'.\n", len(sm.state.Links), stateFileName)
	if sm.state.BaseHost == "" && sm.state.StartURL != "" {
		LbaseURL, err := url.Parse(sm.state.StartURL)
		if err == nil {
			sm.state.BaseHost = LbaseURL.Host
		}
	}
	// Reset transient status on load
	for urlStr, info := range sm.state.Links {
		info.Status = StatusDiscovered // Default starting point if not processed
		if info.Downloaded {
			info.Status = StatusDownloaded
		} else if info.Error != "" {
			// Check specific error messages or status codes from saved state
			if strings.Contains(info.Error, "Blocked by") {
				info.Status = StatusBlocked
			} else if strings.HasPrefix(info.Error, "Redirected") || (info.StatusCode >= 300 && info.StatusCode <= 399) {
				info.Status = StatusRedirected
			} else {
				info.Status = StatusError
			}
		}
		// Clear local path if not downloaded or redirected or blocked, might be stale
		if !info.Downloaded && info.Status != StatusRedirected && info.Status != StatusBlocked {
			info.LocalPath = ""
		} else if info.Status == StatusBlocked {
			info.LocalPath = "" // Ensure blocked state has no local path
		}
		sm.state.Links[urlStr] = info
	}
}
func (sm *StateManager) saveState() {
	sm.mu.RLock()
	sm.state.LastUpdated = time.Now()
	// Create a deep copy for saving to avoid race conditions with transient fields
	stateCopy := *sm.state
	stateCopy.Links = make(map[string]*LinkInfo)
	for k, v := range sm.state.Links {
		linkCopy := *v
		linkCopy.Status = 0 // Don't save transient status
		stateCopy.Links[k] = &linkCopy
	}
	sm.mu.RUnlock()

	data, err := json.MarshalIndent(&stateCopy, "", "  ")
	if err != nil {
		log.Printf("Error marshalling state: %v", err)
		return
	}
	// Use temporary file and rename for atomicity
	tempFileName := stateFileName + ".tmp"
	err = os.WriteFile(tempFileName, data, 0644)
	if err != nil {
		log.Printf("Error writing temporary state file '%s': %v", tempFileName, err)
		os.Remove(tempFileName) // Clean up temp file on error
		return
	}
	err = os.Rename(tempFileName, stateFileName)
	if err != nil {
		log.Printf("Error renaming state file '%s' to '%s': %v", tempFileName, stateFileName, err)
		os.Remove(tempFileName) // Clean up temp file if rename fails
	}
}

// AddLink returns the link info and true if it was newly added
func (sm *StateManager) AddLink(urlStr string, isAssetHint bool) (*LinkInfo, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	parsed, err := url.Parse(urlStr)
	if err == nil {
		parsed.Fragment = ""
		urlStr = parsed.String()
	}
	if existingInfo, exists := sm.state.Links[urlStr]; exists {
		// If we newly discover it as an asset, update the hint
		if isAssetHint && !existingInfo.IsAsset {
			existingInfo.IsAsset = true
		}
		return existingInfo, false // Not newly added
	}
	newInfo := &LinkInfo{
		URL:          urlStr,
		Downloaded:   false,
		IsAsset:      isAssetHint, // Use the hint provided
		Status:       StatusDiscovered,
		DiscoveredAt: time.Now(),
	}
	sm.state.Links[urlStr] = newInfo
	return newInfo, true // Newly added
}

// NeedsProcessing checks if a URL hasn't been successfully processed yet
func (sm *StateManager) NeedsProcessing(urlStr string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	info, exists := sm.state.Links[urlStr]
	// Needs processing if it exists, isn't downloaded, has no permanent error,
	// isn't a redirect, and isn't blocked.
	return exists && !info.Downloaded && info.Error == "" &&
		info.Status != StatusRedirected && info.Status != StatusBlocked
}

// UpdateLinkStatus updates only the transient TUI status
func (sm *StateManager) UpdateLinkStatus(urlStr string, status LinkStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if info, exists := sm.state.Links[urlStr]; exists {
		// Only update if the status is progressing (avoid overwriting final state like Downloaded with Queued)
		// Allow updating *to* a final state however.
		isFinalStatus := status == StatusDownloaded || status == StatusError || status == StatusRedirected || status == StatusBlocked
		if isFinalStatus || info.Status < status {
			info.Status = status
		}
	}
}

// UpdateLinkFull updates the entire record after processing attempt
func (sm *StateManager) UpdateLinkFull(info *LinkInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	info.ProcessedAt = time.Now()

	// Determine final status based on outcome
	if info.Error != "" {
		if strings.Contains(info.Error, "Blocked by") {
			info.Status = StatusBlocked
			info.Downloaded = false // Ensure downloaded is false if blocked
			info.LocalPath = ""     // Ensure no local path if blocked
		} else if info.StatusCode >= 300 && info.StatusCode <= 399 && strings.Contains(info.Error, "Redirected") {
			info.Status = StatusRedirected
			info.Downloaded = false // Ensure downloaded is false if redirected
		} else {
			info.Status = StatusError
			info.Downloaded = false // Ensure downloaded is false on error
			info.LocalPath = ""     // Ensure no local path on error
		}
	} else if info.Downloaded {
		info.Status = StatusDownloaded
		info.Error = "" // Clear error field on successful download
	} else if info.StatusCode >= 300 && info.StatusCode <= 399 {
		info.Status = StatusRedirected
		info.Downloaded = false // Ensure downloaded is false if redirected
		if info.Error == "" {   // Ensure Error field reflects redirect status if not set by download error
			info.Error = fmt.Sprintf("Redirected (%d)", info.StatusCode)
		}
	} else if info.StatusCode != 0 && info.StatusCode != http.StatusOK {
		// Catch other non-OK statuses that didn't result in an explicit error during download
		info.Status = StatusError
		info.Downloaded = false // Ensure downloaded is false
		info.LocalPath = ""     // Ensure no local path
		if info.Error == "" {
			info.Error = fmt.Sprintf("HTTP status %d", info.StatusCode)
		}
	}
	// Don't overwrite IsAsset flag if it was already set by content type check
	// The initial hint remains if content type wasn't checked or didn't override it.

	sm.state.Links[info.URL] = info // Update the map with the modified info
}

func (sm *StateManager) GetLinkInfo(urlStr string) (*LinkInfo, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	info, exists := sm.state.Links[urlStr]
	if !exists {
		return nil, false
	}
	// Return a copy to prevent race conditions in callers modifying it
	infoCopy := *info
	return &infoCopy, true
}

// GetStats calculates current crawl statistics (Added Blocked count)
func (sm *StateManager) GetStats() (total, processing, downloadedHTML, downloadedAssets, redirected, errors, blocked int) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	total = len(sm.state.Links)
	for _, info := range sm.state.Links {
		// Count processing based on transient status
		if info.Status >= StatusProcessing && info.Status < StatusDownloaded {
			processing++
		}

		// Count final states based on persisted info
		if info.Status == StatusBlocked {
			blocked++
		} else if info.Status == StatusRedirected || (info.StatusCode >= 300 && info.StatusCode <= 399 && info.LocalPath != "" && !info.Downloaded) {
			redirected++
		} else if info.Status == StatusError || (info.Error != "" && !info.Downloaded) {
			// Avoid double-counting Blocked as Error
			if !strings.Contains(info.Error, "Blocked by") {
				errors++
			}
		} else if info.Downloaded {
			contentTypeLower := strings.ToLower(info.ContentType)
			if strings.HasPrefix(contentTypeLower, "text/html") {
				downloadedHTML++
			} else if info.IsAsset || !strings.HasPrefix(contentTypeLower, "text/") {
				downloadedAssets++
			} else {
				downloadedAssets++ // Count other text types as assets for now
			}
		}
	}
	return
}

func (sm *StateManager) GetAllLinks() map[string]*LinkInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	linksCopy := make(map[string]*LinkInfo)
	for k, v := range sm.state.Links {
		linkCopy := *v
		linksCopy[k] = &linkCopy
	}
	return linksCopy
}

// --- Bubble Tea TUI ---

// Messages
type urlUpdateMsg struct {
	url         string
	status      LinkStatus
	err         error
	target      string // For redirects
	contentType string // Actual content type after download
}
type urlDiscoveredMsg struct {
	url     string
	isAsset bool // The initial hint
}

// Added 'blocked'
type statsUpdateMsg struct {
	total, processing, downloadedHTML, downloadedAssets, redirected, errors, blocked int
}
type queueSizeUpdateMsg struct {
	discoveryQueueSize int
	downloadQueueSize  int
}
type crawlDoneMsg struct{}

// Model (Added 'blocked' stat)
type TUIModel struct {
	stateManager       *StateManager
	activeJobs         map[string]LinkStatus // URL -> Status
	recentEvents       []string              // Formatted event strings
	stats              statsUpdateMsg        // Includes blocked count now
	width, height      int
	crawlFinished      bool
	discoveryQueueSize int
	downloadQueueSize  int
	discoveryQueueCap  int
	downloadQueueCap   int
}

// Updated initialModel to get blocked count
func initialModel(sm *StateManager, discCap, dlCap int) TUIModel {
	m := TUIModel{
		stateManager:      sm,
		activeJobs:        make(map[string]LinkStatus),
		recentEvents:      make([]string, 0, maxRecentEvents+1),
		discoveryQueueCap: discCap,
		downloadQueueCap:  dlCap,
	}
	m.stats.total, m.stats.processing, m.stats.downloadedHTML, m.stats.downloadedAssets, m.stats.redirected, m.stats.errors, m.stats.blocked = sm.GetStats()
	return m
}

func (m TUIModel) Init() tea.Cmd { return nil }

// Updated Update handler
func (m TUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			return m, tea.Quit
		}
	case urlDiscoveredMsg:
		event := fmt.Sprintf("[+] Discovered%s: %s", map[bool]string{true: " Asset", false: " Page"}[msg.isAsset], msg.url)
		m.addRecentEvent(event)
		// Stats.total updated by ticker
	case urlUpdateMsg:
		// Update active jobs map
		isFinalStatus := msg.status == StatusDownloaded || msg.status == StatusError || msg.status == StatusRedirected || msg.status == StatusBlocked
		if isFinalStatus {
			delete(m.activeJobs, msg.url)
		} else {
			// Only add/update if it's an active processing status
			if msg.status >= StatusProcessing && msg.status < StatusDownloaded {
				m.activeJobs[msg.url] = msg.status
			} else if msg.status == StatusQueuedDiscovery || msg.status == StatusQueuedDownload {
				// Don't show queued items in active jobs, remove if it was previously active
				delete(m.activeJobs, msg.url)
			}
		}

		// Add event message for significant status changes
		var event string
		if msg.err != nil {
			// Don't log redirect errors handled gracefully or known blocked status
			if msg.status == StatusError && !strings.Contains(msg.err.Error(), "Blocked by") {
				event = fmt.Sprintf("[✗] Error %s: %v", msg.url, msg.err)
			} else if msg.status == StatusBlocked { // Generate event specifically for blocked
				event = fmt.Sprintf("[!] Blocked  : %s (%v)", msg.url, msg.err)
			} else if msg.status == StatusRedirected && msg.target != "" { // Ensure redirect event is logged even if error is set internally
				event = fmt.Sprintf("[→] Redirect : %s → %s", msg.url, msg.target)
			}
		} else { // No error case
			switch msg.status {
			case StatusQueuedDiscovery:
				event = fmt.Sprintf("[Q Disc] Queued: %s", msg.url)
			case StatusQueuedDownload:
				event = fmt.Sprintf("[Q Dload] Queued: %s", msg.url)
			case StatusDownloaded:
				dlType := "Unknown"
				if msg.contentType != "" {
					contentTypeLower := strings.ToLower(msg.contentType)
					if strings.HasPrefix(contentTypeLower, "text/html") {
						dlType = "HTML"
					} else {
						dlType = "Asset" // Simplified TUI categorization
					}
				}
				event = fmt.Sprintf("[✓] Done (%s): %s", dlType, msg.url)
			case StatusRedirected:
				event = fmt.Sprintf("[→] Redirect : %s → %s", msg.url, msg.target)
			case StatusBlocked: // Should be caught by err != nil, but as fallback
				event = fmt.Sprintf("[!] Blocked (?) : %s", msg.url)
			case StatusError: // Fallback if err was nil but status is Error
				event = fmt.Sprintf("[✗] Error (?) : %s", msg.url)
			}
		}
		if event != "" {
			m.addRecentEvent(event)
		}

	case statsUpdateMsg:
		m.stats = msg // Update main stats from ticker (now includes blocked)
	case queueSizeUpdateMsg:
		m.discoveryQueueSize = msg.discoveryQueueSize
		m.downloadQueueSize = msg.downloadQueueSize
	case crawlDoneMsg:
		m.crawlFinished = true
		m.addRecentEvent("--- Crawl Finished ---")
		// Refresh stats one last time (includes blocked)
		m.stats.total, m.stats.processing, m.stats.downloadedHTML, m.stats.downloadedAssets, m.stats.redirected, m.stats.errors, m.stats.blocked = m.stateManager.GetStats()
		m.activeJobs = make(map[string]LinkStatus) // Clear active jobs
		m.discoveryQueueSize = 0
		m.downloadQueueSize = 0
	}
	return m, nil
}

// addRecentEvent (Modified for URL length clipping) - No changes needed here
func (m *TUIModel) addRecentEvent(event string) {
	maxUrlLen := m.width - 25 // Estimate space for prefix/suffix
	if maxUrlLen < 20 {
		maxUrlLen = 20
	}

	parts := strings.SplitN(event, ": ", 2)
	if len(parts) == 2 {
		prefix := parts[0]
		content := parts[1]

		// Clip URLs within the content
		if strings.Contains(content, " -> ") { // Redirect case
			urls := strings.SplitN(content, " -> ", 2)
			if len(urls[0]) > maxUrlLen {
				urls[0] = urls[0][:maxUrlLen-3] + "..."
			}
			if len(urls[1]) > maxUrlLen {
				urls[1] = urls[1][:maxUrlLen-3] + "..."
			}
			content = urls[0] + " -> " + urls[1]
		} else { // Other cases (Discovered, Done, Error, Queued, Blocked)
			// Find potential single URL (simple check)
			if strings.HasPrefix(content, "http://") || strings.HasPrefix(content, "https://") {
				urlEndIndex := strings.Index(content, " ") // Find first space after URL start
				if urlEndIndex == -1 {
					urlEndIndex = len(content)
				} // If no space, URL is the rest

				potentialUrl := content[:urlEndIndex]
				if len(potentialUrl) > maxUrlLen {
					suffix := ""
					if urlEndIndex < len(content) {
						suffix = content[urlEndIndex:]
					}
					potentialUrl = potentialUrl[:maxUrlLen-3] + "..."
					content = potentialUrl + suffix
				}
				// Handle error/blocked messages separately to avoid excessive truncation of the message itself
				if (strings.HasPrefix(prefix, "[✗] Error") || strings.HasPrefix(prefix, "[!] Blocked")) && len(content) > m.width-10 {
					content = content[:m.width-10] + "..."
				}

			} else if len(content) > m.width-10 { // General fallback clipping
				content = content[:m.width-10] + "..."
			}
		}
		event = prefix + ": " + content
	} else { // Fallback if split fails (clip whole event)
		if len(event) > m.width-4 {
			event = event[:m.width-4] + "..."
		}
	}

	m.recentEvents = append(m.recentEvents, event)
	if len(m.recentEvents) > maxRecentEvents {
		// Efficiently trim slice from the beginning
		copy(m.recentEvents[0:], m.recentEvents[len(m.recentEvents)-maxRecentEvents:])
		m.recentEvents = m.recentEvents[:maxRecentEvents]
	}
}

// getProgressIndicator (Unchanged) - Blocked status won't show here
func getProgressIndicator(status LinkStatus) string {
	totalSteps := 4 // Processing(1), Downloading(2), Parsing(3) -> Done(4)
	currentStep := 0
	switch status {
	// Queued statuses won't typically be shown in active jobs, but handle defensively
	case StatusQueuedDiscovery, StatusQueuedDownload:
		currentStep = 0
	case StatusProcessing:
		currentStep = 1
	case StatusDownloading:
		currentStep = 2
	case StatusParsing:
		currentStep = 3
	// Final states might flash briefly if update is slow
	case StatusDownloaded, StatusRedirected, StatusError, StatusBlocked:
		currentStep = totalSteps
	}
	barWidth := 7
	filled := (currentStep * barWidth) / totalSteps
	empty := barWidth - filled
	if filled < 0 {
		filled = 0
	}
	if empty < 0 {
		empty = 0
	}
	// Prevent overflow if currentStep somehow exceeds totalSteps
	if filled > barWidth {
		filled = barWidth
	}
	if empty < 0 {
		empty = 0
	}

	return "[" + strings.Repeat("=", filled) + strings.Repeat("-", empty) + "]"
}

// View (Updated stats line for Blocked count)
func (m TUIModel) View() string {
	var s strings.Builder
	numDiscWorkers := numTotalWorkers - minDownloadWorkers
	if numDiscWorkers < 1 {
		numDiscWorkers = 1
	} // Ensure at least one discovery worker
	numDlWorkers := numTotalWorkers - numDiscWorkers

	s.WriteString("--- Go Web Crawler --- (Press q or Ctrl+C to quit) ---\n")
	s.WriteString(fmt.Sprintf(" Discovery Queue: %-5d / %-5d (W:%d) | Download Queue: %-5d / %-5d (W:%d) | Delay: %v\n",
		m.discoveryQueueSize, m.discoveryQueueCap, numDiscWorkers,
		m.downloadQueueSize, m.downloadQueueCap, numDlWorkers, requestDelay))
	// Updated stats line format
	s.WriteString(fmt.Sprintf(" Stats: Total: %-5d Processing: %-4d HTML: %-5d Assets: %-5d Redirects: %-5d Errors: %-4d Blocked: %d\n",
		m.stats.total, m.stats.processing, m.stats.downloadedHTML, m.stats.downloadedAssets, m.stats.redirected, m.stats.errors, m.stats.blocked))
	s.WriteString(strings.Repeat("-", m.width) + "\n")

	s.WriteString("Active Tasks:\n")
	activeJobUrls := make([]string, 0, len(m.activeJobs))
	for url := range m.activeJobs {
		activeJobUrls = append(activeJobUrls, url)
	}
	sort.Strings(activeJobUrls)

	activeCount := 0
	for _, url := range activeJobUrls {
		if activeCount >= maxActiveDisplay {
			s.WriteString(fmt.Sprintf(" ... and %d more\n", len(activeJobUrls)-activeCount))
			break
		}
		status := m.activeJobs[url]
		statusStr := ""
		// Map active statuses to display strings
		switch status {
		case StatusProcessing:
			statusStr = "Processing"
		case StatusDownloading:
			statusStr = "Downloading"
		case StatusParsing:
			statusStr = "Parsing   " // Pad for alignment
		default:
			statusStr = "Unknown   " // Should not happen often
		}

		progress := getProgressIndicator(status)
		displayURL := url
		maxURLLen := m.width - 28 // Adjust for status + progress + padding
		if maxURLLen < 20 {
			maxURLLen = 20
		}
		if len(url) > maxURLLen {
			displayURL = url[:maxURLLen-3] + "..."
		}
		s.WriteString(fmt.Sprintf(" %-11s %s %s\n", statusStr, progress, displayURL))
		activeCount++
	}
	if activeCount == 0 && !m.crawlFinished {
		s.WriteString(" (Waiting for tasks...)\n")
	}
	if m.crawlFinished && activeCount == 0 {
		s.WriteString(" (None)\n")
	}

	s.WriteString(strings.Repeat("-", m.width) + "\n")
	s.WriteString("Recent Events:\n")

	// Calculate available lines for events dynamically
	headerLines := 3 // Title, Queue Info, Stats
	separatorLines := 2
	activeHeaderLine := 1
	activeFooterLine := 0 // If "and more" is shown
	if activeCount > maxActiveDisplay {
		activeFooterLine = 1
	}
	if activeCount == 0 {
		activeCount = 1
	} // Account for "(Waiting)" line
	eventsHeaderLine := 1
	finalMessageLine := 0
	if m.crawlFinished {
		finalMessageLine = 2
	} // Account for "Crawl Finished" + blank line

	availableHeight := m.height
	usedHeight := headerLines + separatorLines + activeHeaderLine + activeCount + activeFooterLine + eventsHeaderLine + finalMessageLine
	maxEventLines := availableHeight - usedHeight
	if maxEventLines < 1 {
		maxEventLines = 1
	} // Show at least one event if possible
	if maxEventLines > maxRecentEvents {
		maxEventLines = maxRecentEvents
	}

	if len(m.recentEvents) == 0 {
		s.WriteString(" (No events yet)\n")
	} else {
		displayCount := 0
		startIdx := len(m.recentEvents) - 1
		for i := startIdx; i >= 0; i-- {
			if displayCount >= maxEventLines {
				break
			}
			s.WriteString(" " + m.recentEvents[i] + "\n")
			displayCount++
		}
	}

	if m.crawlFinished {
		s.WriteString("\nCrawl finished. Press q or Ctrl+C to exit.")
	}

	return s.String()
}

// --- Setup Logging (Unchanged) ---
func setupLogging() *os.File {
	fmt.Fprintf(os.Stderr, "Logging detailed progress to: %s\n", logFileName)
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file %s: %v", logFileName, err)
	}
	log.SetOutput(file)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("--- Log Started ---")
	return file
}

// --- Main (Updated Ticker Goroutine) ---
func main() {
	logFile := setupLogging()
	defer logFile.Close()
	defer log.Println("--- Log Ended ---")

	// --- Initial Setup ---
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <start_url>\n", os.Args[0])
		os.Exit(1)
	}
	startURLArg := os.Args[1]
	baseURL, err := url.Parse(startURLArg)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		fmt.Fprintf(os.Stderr, "Invalid starting URL: %v\n", err)
		os.Exit(1)
	}
	baseDomainURL := &url.URL{Scheme: baseURL.Scheme, Host: baseURL.Host}
	baseURL.Fragment = "" // Normalize URL
	normalizedStartURL := baseURL.String()

	err = os.MkdirAll(downloadBaseDir, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create download directory '%s': %v\n", downloadBaseDir, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Starting crawl from: %s\n", normalizedStartURL)
	fmt.Fprintf(os.Stderr, "Downloading pages to: %s/\n", downloadBaseDir)

	// --- Calculate Worker Distribution ---
	var numActualDownloadWorkers int
	var numDiscoveryWorkers int

	if numTotalWorkers <= 0 { // Handle invalid total worker count
		numDiscoveryWorkers = 1
		numActualDownloadWorkers = 0
		log.Println("Warning: numTotalWorkers <= 0, defaulting to 1 discovery worker.")
	} else if minDownloadWorkers <= 0 { // Handle invalid min download workers
		// Assign at least one download worker if possible, otherwise all to discovery
		numActualDownloadWorkers = 1
		if numTotalWorkers == 1 { // Edge case: only 1 total worker
			numActualDownloadWorkers = 0
			numDiscoveryWorkers = 1
		} else {
			numDiscoveryWorkers = numTotalWorkers - numActualDownloadWorkers
		}
		log.Println("Warning: minDownloadWorkers <= 0, assigning 1 download worker if possible.")
	} else if minDownloadWorkers >= numTotalWorkers {
		// Not enough workers for the minimum downloaders. Prioritize discovery with at least 1.
		numDiscoveryWorkers = 1
		numActualDownloadWorkers = numTotalWorkers - 1
		if numActualDownloadWorkers < 0 {
			numActualDownloadWorkers = 0
		} // Ensure non-negative
		log.Printf("Warning: minDownloadWorkers (%d) >= numTotalWorkers (%d). Adjusting distribution to %d discovery, %d download.", minDownloadWorkers, numTotalWorkers, numDiscoveryWorkers, numActualDownloadWorkers)
	} else {
		// Standard case: We have enough workers
		numActualDownloadWorkers = minDownloadWorkers
		numDiscoveryWorkers = numTotalWorkers - numActualDownloadWorkers
	}

	// Ensure at least one discovery worker if possible after calculations
	if numDiscoveryWorkers <= 0 && numTotalWorkers > 0 {
		numDiscoveryWorkers = 1
		numActualDownloadWorkers = numTotalWorkers - 1
		if numActualDownloadWorkers < 0 {
			numActualDownloadWorkers = 0
		}
		log.Printf("Adjusted distribution to ensure at least 1 discovery worker.")
	}

	fmt.Fprintf(os.Stderr, "Worker distribution: %d Discovery, %d Download (Total: %d, Min Download Req: %d)\n",
		numDiscoveryWorkers, numActualDownloadWorkers, numTotalWorkers, minDownloadWorkers)

	// --- Initialize State and TUI ---
	stateManager := NewStateManager(baseURL)
	tuiModel := initialModel(stateManager, discoveryQueueBuffer, downloadQueueBuffer)
	tuiProgram := tea.NewProgram(tuiModel, tea.WithAltScreen())

	// --- Concurrency Setup ---
	discoveryQueue := make(chan string, discoveryQueueBuffer)
	downloadQueue := make(chan string, downloadQueueBuffer)
	var wg sync.WaitGroup
	tuiFinished := make(chan struct{})
	quitSignal := make(chan struct{}) // Single signal to stop all workers

	// --- Start TUI ---
	go func() {
		// f, _ := tea.LogToFile("bubbletea_debug.log", "debug") // Uncomment for TUI debugging
		// defer f.Close()
		_, err := tuiProgram.Run()
		if err != nil {
			log.Printf("TUI Error: %v", err)
		}
		close(quitSignal)  // Signal workers to stop
		close(tuiFinished) // Signal main TUI has exited
	}()

	// --- Periodic Tickers (Updated stats send) ---
	saveTicker := time.NewTicker(saveStateInterval)
	defer saveTicker.Stop()
	statsTicker := time.NewTicker(statsUpdateInterval)
	defer statsTicker.Stop()
	go func() {
		for {
			select {
			case <-saveTicker.C:
				stateManager.saveState()
			case <-statsTicker.C:
				// Fetch all stats including blocked
				total, processing, downloadedHTML, downloadedAssets, redirected, errors, blocked := stateManager.GetStats()
				// Send updated message
				tuiProgram.Send(statsUpdateMsg{total, processing, downloadedHTML, downloadedAssets, redirected, errors, blocked})
				tuiProgram.Send(queueSizeUpdateMsg{
					discoveryQueueSize: len(discoveryQueue),
					downloadQueueSize:  len(downloadQueue),
				})
			case <-quitSignal:
				log.Println("Tickers stopping due to quit signal.")
				return
			}
		}
	}()

	// --- Seed Initial Queues ---
	log.Println("--- Seeding Queues ---")
	_, added := stateManager.AddLink(normalizedStartURL, false) // Assume start URL is HTML initially
	if added {
		tuiProgram.Send(urlDiscoveredMsg{url: normalizedStartURL, isAsset: false})
	}

	initialQueueCount := 0
	if stateManager.NeedsProcessing(normalizedStartURL) {
		log.Printf("Queueing initial URL '%s' for discovery.", normalizedStartURL)
		wg.Add(1)
		stateManager.UpdateLinkStatus(normalizedStartURL, StatusQueuedDiscovery)
		tuiProgram.Send(urlUpdateMsg{url: normalizedStartURL, status: StatusQueuedDiscovery})
		select {
		case discoveryQueue <- normalizedStartURL:
			initialQueueCount++
		default:
			log.Printf("Failed to queue initial URL (discovery queue full?): %s", normalizedStartURL)
			wg.Done() // Decrement WG if initial queue fails
		}
	} else {
		// If start URL doesn't need processing, seed from existing state
		info, _ := stateManager.GetLinkInfo(normalizedStartURL) // Get info to log why seeding needed
		log.Printf("Start URL %s already processed (Status: %d, Error: %s). Seeding from existing state...", normalizedStartURL, info.Status, info.Error)
		allLinks := stateManager.GetAllLinks()
		queuedCount := 0
		for urlStr, linkInfo := range allLinks {
			if stateManager.NeedsProcessing(urlStr) {
				linkURL, err := url.Parse(urlStr)
				// Only queue internal links that need processing
				if err == nil && isInternal(linkURL, baseDomainURL) {
					if queuedCount < (discoveryQueueBuffer + downloadQueueBuffer) { // Limit initial seeding
						wg.Add(1)
						if linkInfo.IsAsset { // Use IsAsset hint from loaded state
							stateManager.UpdateLinkStatus(urlStr, StatusQueuedDownload)
							tuiProgram.Send(urlUpdateMsg{url: urlStr, status: StatusQueuedDownload})
							select {
							case downloadQueue <- urlStr:
								queuedCount++
							case <-quitSignal:
								log.Printf("Quit signal during seeding download queue for: %s", urlStr)
								wg.Done()
								goto endSeedLoop // Use goto to break out of outer loop
							default:
								log.Printf("Failed to seed download queue (full?): %s", urlStr)
								wg.Done()
							}
						} else {
							stateManager.UpdateLinkStatus(urlStr, StatusQueuedDiscovery)
							tuiProgram.Send(urlUpdateMsg{url: urlStr, status: StatusQueuedDiscovery})
							select {
							case discoveryQueue <- urlStr:
								queuedCount++
							case <-quitSignal:
								log.Printf("Quit signal during seeding discovery queue for: %s", urlStr)
								wg.Done()
								goto endSeedLoop // Use goto to break out of outer loop
							default:
								log.Printf("Failed to seed discovery queue (full?): %s", urlStr)
								wg.Done()
							}
						}
					} else {
						log.Printf("Seeding limit reached, skipping: %s", urlStr)
						// No wg.Add/Done needed if skipped before trying to queue
					}
				}
			}
		}
	endSeedLoop:
		initialQueueCount = queuedCount
		log.Printf("Seeded queues with %d previously discovered, unprocessed URLs.", initialQueueCount)
	}
	log.Printf("Finished seeding. Discovery Queue: %d, Download Queue: %d", len(discoveryQueue), len(downloadQueue))
	log.Println("--- Starting Workers ---")

	// --- Start Workers ---
	for i := 0; i < numDiscoveryWorkers; i++ {
		go discoveryWorker(i, discoveryQueue, downloadQueue, &wg, stateManager, baseDomainURL, tuiProgram, quitSignal)
	}
	for i := 0; i < numActualDownloadWorkers; i++ {
		go downloadWorker(numDiscoveryWorkers+i, downloadQueue, discoveryQueue, &wg, stateManager, baseDomainURL, tuiProgram, quitSignal) // Pass both queues for redirects
	}

	// --- Wait for Crawl Completion ---
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		log.Println("WaitGroup finished naturally.")
	case <-quitSignal:
		log.Println("Quit signal received, waiting for active tasks to finish...")
		select {
		case <-waitDone:
			log.Println("WaitGroup finished after quit signal.")
		case <-time.After(10 * time.Second): // Increased timeout
			log.Println("Timeout waiting for WaitGroup after quit signal.")
		}
	}

	// --- Finalize ---
	log.Println("Closing queues...")
	// It's generally safe to close channels even if goroutines are still reading,
	// they will get the zero value and the 'ok' bool will be false.
	// Workers should handle the 'ok' value correctly.
	close(discoveryQueue)
	close(downloadQueue)

	log.Println("Sending crawl done message to TUI and saving final state.")
	tuiProgram.Send(crawlDoneMsg{})    // Notify TUI crawl logic is done
	stateManager.saveState()           // Final save
	time.Sleep(500 * time.Millisecond) // Give TUI a bit more time to render final stats

	fmt.Fprintln(os.Stderr, "Waiting for TUI to exit...")
	<-tuiFinished // Wait for the TUI Run() goroutine to finish
	fmt.Fprintln(os.Stderr, "TUI finished.")

	// --- Print final summary to stderr (Updated stats) ---
	fmt.Fprintf(os.Stderr, "\n--- Crawl Summary ---\n")
	total, _, downloadedHTML, downloadedAssets, redirected, errors, blocked := stateManager.GetStats()
	fmt.Fprintf(os.Stderr, "Discovered: %d | HTML: %d | Assets: %d | Redirects: %d | Errors: %d | Blocked: %d\n",
		total, downloadedHTML, downloadedAssets, redirected, errors, blocked)
	fmt.Fprintf(os.Stderr, "State saved to: %s\n", stateFileName)
	fmt.Fprintf(os.Stderr, "Files downloaded to: %s/\n", downloadBaseDir)
	fmt.Fprintf(os.Stderr, "Detailed logs in: %s\n", logFileName)
}

// --- Discovery Worker ---
func discoveryWorker(id int, discQueue chan string, dlQueue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL, tui *tea.Program, quit <-chan struct{}) {
	log.Printf("Discovery Worker %d started", id)
	defer log.Printf("Discovery Worker %d stopped", id)
	for {
		select {
		case urlStr, ok := <-discQueue:
			if !ok {
				return // Queue closed
			}
			log.Printf("Discovery Worker %d: Dequeued: %s", id, urlStr)
			processDiscoveryURL(id, urlStr, discQueue, dlQueue, wg, sm, baseDomainURL, tui, quit)
		case <-quit:
			return
		}
	}
}

// --- Download Worker ---
func downloadWorker(id int, dlQueue chan string, discQueue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL, tui *tea.Program, quit <-chan struct{}) {
	log.Printf("Download Worker %d started", id)
	defer log.Printf("Download Worker %d stopped", id)
	for {
		select {
		case urlStr, ok := <-dlQueue:
			if !ok {
				return // Queue closed
			}
			log.Printf("Download Worker %d: Dequeued: %s", id, urlStr)
			processDownloadURL(id, urlStr, discQueue, dlQueue, wg, sm, baseDomainURL, tui, quit) // Pass both queues
		case <-quit:
			return
		}
	}
}

// --- Helper function to detect Cloudflare challenge ---
func isCloudflareChallenge(body io.Reader) (isBlocked bool, readErr error, readBytes []byte) {
	// Read a small chunk to check for common Cloudflare patterns
	buffer := make([]byte, 4096) // Read first 4KB
	n, err := io.ReadFull(body, buffer)

	// We expect io.ErrUnexpectedEOF if file is smaller than buffer, that's okay for detection.
	// Only return error if *nothing* could be read (n=0 and not EOF) or a different error occurred.
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return false, fmt.Errorf("reading body prefix failed: %w", err), nil
	}
	if n == 0 && err == io.EOF { // Empty body is not blocked
		return false, nil, nil
	}
	if n == 0 { // Could not read anything, error case handled above
		return false, err, nil // Return original error if n=0 but not EOF
	}

	content := string(buffer[:n])
	readBytes = buffer[:n] // Return the bytes read

	// Simple checks for common Cloudflare challenge page elements
	if strings.Contains(content, "<title>Just a moment...</title>") ||
		strings.Contains(content, "Enable JavaScript and cookies to continue") ||
		strings.Contains(content, "cdn-cgi/challenge-platform") ||
		strings.Contains(content, "window._cf_chl_opt") ||
		strings.Contains(content, `id="challenge-error-text"`) {
		return true, nil, readBytes // Is blocked, no error during check itself
	}

	// If read was successful but pattern not found, return false
	// Return the original read error (EOF/UnexpectedEOF) if it occurred, as it's informational
	return false, err, readBytes // Not blocked
}

// --- processDiscoveryURL (Handles HTML download, parsing, and queuing new links) ---
// ADDED: Discovery Queue Capacity Check
func processDiscoveryURL(workerID int, urlStr string, discQueue chan string, dlQueue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL, tui *tea.Program, quit <-chan struct{}) {
	defer wg.Done()

	select {
	case <-quit:
		log.Printf("Worker %d: Quitting before processing %s", workerID, urlStr)
		return
	default:
	}

	if !sm.NeedsProcessing(urlStr) {
		return
	}

	if requestDelay > 0 {
		select {
		case <-time.After(requestDelay):
		case <-quit:
			log.Printf("Worker %d: Quitting during delay for %s", workerID, urlStr)
			return
		}
	}

	currentInfo, exists := sm.GetLinkInfo(urlStr)
	if !exists {
		log.Printf("Worker %d: ERROR - LinkInfo missing for %s during processing", workerID, urlStr)
		return
	}

	sm.UpdateLinkStatus(urlStr, StatusProcessing)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusProcessing})
	sm.UpdateLinkStatus(urlStr, StatusDownloading)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloading})

	log.Printf("Worker %d: Downloading (discovery): %s", workerID, urlStr)
	resultPath, contentType, statusCode, downloadErr := downloadResource(urlStr, baseDomainURL.Host)
	log.Printf("Worker %d: Download finished for %s (Status: %d, CT: %s, Path: %s, Err: %v)", workerID, urlStr, statusCode, contentType, resultPath, downloadErr)

	currentInfo.StatusCode = statusCode
	currentInfo.ContentType = contentType
	currentInfo.ProcessedAt = time.Now()

	// Handle various download outcomes (Errors, Redirects, Non-OK, Cloudflare)
	// ... (existing error handling, redirect, non-OK, cloudflare check logic remains the same) ...
	// 1. Hard Error (non-redirect)
	if downloadErr != nil && !(statusCode >= 300 && statusCode <= 399) {
		log.Printf("Worker %d: Download Error for %s: %v", workerID, urlStr, downloadErr)
		currentInfo.Error = downloadErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: downloadErr})
		return
	}
	// 2. Redirect (Status 3xx)
	if statusCode >= 300 && statusCode <= 399 {
		redirectTargetURL := resultPath
		log.Printf("Worker %d: Redirect detected for %s -> %s", workerID, urlStr, redirectTargetURL)
		currentInfo.Error = fmt.Sprintf("Redirected (%d)", statusCode)
		currentInfo.Downloaded = false
		currentInfo.LocalPath = redirectTargetURL
		currentInfo.Status = StatusRedirected
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusRedirected, target: redirectTargetURL, contentType: contentType})
		handleRedirectTarget(workerID, redirectTargetURL, currentInfo.IsAsset, baseDomainURL, sm, discQueue, dlQueue, wg, tui, quit)
		return
	}
	// 3. Non-OK, Non-Redirect Status (4xx, 5xx)
	if statusCode != http.StatusOK {
		err := fmt.Errorf("HTTP status %d", statusCode)
		if downloadErr == nil {
			downloadErr = err
		} else if !strings.Contains(downloadErr.Error(), fmt.Sprintf("%d", statusCode)) {
			downloadErr = fmt.Errorf("%w; explicit status %d", downloadErr, statusCode)
		}
		log.Printf("Worker %d: Non-OK status for %s: %d", workerID, urlStr, statusCode)
		currentInfo.Error = downloadErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		if resultPath != "" {
			os.Remove(resultPath)
		}
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: downloadErr, contentType: contentType})
		return
	}
	// 4. Success (200 OK) --- Check for Cloudflare ---
	file, err := os.Open(resultPath)
	if err != nil {
		log.Printf("Worker %d: Error opening downloaded file %s for Cloudflare check: %v", workerID, resultPath, err)
		currentInfo.Error = fmt.Errorf("error opening file for check: %w", err).Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: err, contentType: contentType})
		if file != nil {
			file.Close()
		}
		return
	}
	bufReader := bufio.NewReader(file)
	isBlocked, checkErr, _ := isCloudflareChallenge(bufReader)
	file.Close()
	if checkErr != nil && checkErr != io.EOF && checkErr != io.ErrUnexpectedEOF {
		log.Printf("Worker %d: Error checking for Cloudflare challenge on %s: %v. Proceeding cautiously.", workerID, urlStr, checkErr)
	}
	if isBlocked {
		log.Printf("Worker %d: Detected Cloudflare challenge page for %s. Skipping parsing.", workerID, urlStr)
		blockErr := errors.New("blocked by Cloudflare challenge")
		currentInfo.Error = blockErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		os.Remove(resultPath)
		currentInfo.Status = StatusBlocked
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusBlocked, err: blockErr})
		return
	}

	// 5. Success (200 OK), NOT Blocked - Check Content Type ---
	currentInfo.Downloaded = true
	currentInfo.Error = ""
	currentInfo.LocalPath = resultPath
	isHTML := strings.HasPrefix(strings.ToLower(contentType), "text/html")
	if !isHTML {
		log.Printf("Worker %d: URL %s downloaded but Content-Type is '%s', not HTML. Treating as asset.", workerID, urlStr, contentType)
		currentInfo.IsAsset = true
		currentInfo.Status = StatusDownloaded
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
		return
	}

	// 6. Success (200 OK), IS HTML, NOT Blocked - Proceed to Parsing ---
	currentInfo.IsAsset = false
	log.Printf("Worker %d: Successfully downloaded HTML %s to %s", workerID, urlStr, resultPath)
	sm.UpdateLinkStatus(urlStr, StatusParsing)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusParsing})

	select {
	case <-quit:
		log.Printf("Worker %d: Quitting before parsing %s", workerID, urlStr)
		currentInfo.Status = StatusDownloaded
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
		return
	default:
	}

	log.Printf("Worker %d: Parsing HTML for links: %s", workerID, urlStr)
	fileForParsing, err := os.Open(resultPath)
	if err != nil {
		log.Printf("Worker %d: Error re-opening downloaded HTML %s for parsing: %v", workerID, resultPath, err)
		currentInfo.Error = fmt.Errorf("error opening HTML for parsing: %w", err).Error()
		currentInfo.Downloaded = true
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: err, contentType: contentType})
		return
	}
	defer fileForParsing.Close()

	pageURL, parseErr := url.Parse(urlStr)
	if parseErr != nil {
		log.Printf("Worker %d: Error parsing page URL %s for link resolution: %v", workerID, urlStr, parseErr)
		currentInfo.Error = fmt.Errorf("error parsing page URL: %w", parseErr).Error()
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: parseErr, contentType: contentType})
		return
	}

	foundLinks := findResourceLinks(fileForParsing, pageURL)
	log.Printf("Worker %d: Found %d potential links/resources in %s", workerID, len(foundLinks), urlStr)

	type LinkToQueue struct {
		URL     string
		IsAsset bool
	}
	var linksToQueue []LinkToQueue

	for _, link := range foundLinks {
		absoluteURL := resolveURL(pageURL, link.URL)
		if absoluteURL == nil {
			continue
		}
		normalizedLinkStr := absoluteURL.String()

		if isInternal(absoluteURL, baseDomainURL) {
			addedInfo, added := sm.AddLink(normalizedLinkStr, link.IsAsset)
			needsQ := sm.NeedsProcessing(normalizedLinkStr)
			if added {
				tui.Send(urlDiscoveredMsg{url: normalizedLinkStr, isAsset: addedInfo.IsAsset})
			}
			if needsQ {
				linkInfo, exists := sm.GetLinkInfo(normalizedLinkStr)
				if exists && linkInfo.Status < StatusQueuedDiscovery {
					linksToQueue = append(linksToQueue, LinkToQueue{URL: normalizedLinkStr, IsAsset: addedInfo.IsAsset})
				}
			}
		}
	}

	// --- Queue Discovered Links (WITH THROTTLING CHECK) ---
	queuedCount := 0
	// Calculate the threshold dynamically based on channel capacity
	discoveryQueueThreshold := cap(discQueue) / 2

	for _, item := range linksToQueue {
		// Apply discovery delay PER item queued
		if discoveryDelay > 0 {
			select {
			case <-time.After(discoveryDelay):
			case <-quit:
				log.Printf("Worker %d: Quitting during discovery delay before queueing %s (from %s)", workerID, item.URL, urlStr)
				currentInfo.Status = StatusDownloaded
				sm.UpdateLinkFull(currentInfo)
				tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
				return // Exit the outer function
			}
		}

		var queueTarget chan string
		var targetStatus LinkStatus

		// *** START THROTTLING LOGIC ***
		if !item.IsAsset { // This is potentially an HTML page link
			currentDiscQueueLen := len(discQueue) // Get current length just before check
			if currentDiscQueueLen >= discoveryQueueThreshold {
				log.Printf("Worker %d: Discovery queue at %d/%d (>=50%%), skipping queuing of page: %s (from %s)",
					workerID, currentDiscQueueLen, cap(discQueue), item.URL, urlStr)
				// *** IMPORTANT: Do NOT wg.Add(1) for skipped items ***
				continue // Skip to the next item in linksToQueue
			}
			// If not throttling, proceed to queue to discoveryQueue
			queueTarget = discQueue
			targetStatus = StatusQueuedDiscovery
		} else { // This is an asset link, queue it regardless of discovery queue fullness
			queueTarget = dlQueue
			targetStatus = StatusQueuedDownload
		}
		// *** END THROTTLING LOGIC ***

		// Update status for the *target* item BEFORE queueing
		sm.UpdateLinkStatus(item.URL, targetStatus)
		tui.Send(urlUpdateMsg{url: item.URL, status: targetStatus})

		wg.Add(1) // Increment *before* trying to send

		select {
		case queueTarget <- item.URL:
			queuedCount++
		case <-quit:
			log.Printf("Worker %d: Quit signal while trying to queue %s (from %s). Decrementing WG.", workerID, item.URL, urlStr)
			wg.Done() // Decrement because item was not queued
			currentInfo.Status = StatusDownloaded
			sm.UpdateLinkFull(currentInfo)
			tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
			return // Exit the outer function
		}
	} // End loop linksToQueue

	log.Printf("Worker %d: Finished queuing links for %s (queued %d new items).", workerID, urlStr, queuedCount)

	// --- Final Update for Successful HTML Processing of urlStr ---
	currentInfo.Status = StatusDownloaded
	sm.UpdateLinkFull(currentInfo)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
	log.Printf("Worker %d: Completed processing %s", workerID, urlStr)
}

// --- processDownloadURL (Handles Asset download ONLY) ---
func processDownloadURL(workerID int, urlStr string, discQueue chan string, dlQueue chan string, wg *sync.WaitGroup, sm *StateManager, baseDomainURL *url.URL, tui *tea.Program, quit <-chan struct{}) {
	defer wg.Done() // Decrement counter when this function returns

	// Check quit signal early
	select {
	case <-quit:
		log.Printf("Worker %d: Quitting before processing download %s", workerID, urlStr)
		return
	default:
	}

	// Check if still needs processing
	if !sm.NeedsProcessing(urlStr) {
		// log.Printf("Worker %d: Skipping already processed or invalid state (download): %s", workerID, urlStr)
		return
	}

	// Apply request delay
	if requestDelay > 0 {
		select {
		case <-time.After(requestDelay):
		case <-quit:
			log.Printf("Worker %d: Quitting during delay for download %s", workerID, urlStr)
			return
		}
	}

	// Get current info (make a copy)
	currentInfo, exists := sm.GetLinkInfo(urlStr)
	if !exists {
		log.Printf("Worker %d: ERROR - LinkInfo missing for %s during download processing", workerID, urlStr)
		return
	}

	// --- Mark as Processing & Downloading ---
	sm.UpdateLinkStatus(urlStr, StatusProcessing)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusProcessing})
	sm.UpdateLinkStatus(urlStr, StatusDownloading)
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloading})

	// --- Download Resource ---
	log.Printf("Worker %d: Downloading (asset/direct): %s", workerID, urlStr)
	resultPath, contentType, statusCode, downloadErr := downloadResource(urlStr, baseDomainURL.Host)
	log.Printf("Worker %d: Download finished for %s (Status: %d, CT: %s, Path: %s, Err: %v)", workerID, urlStr, statusCode, contentType, resultPath, downloadErr)

	// Update info with download results
	currentInfo.StatusCode = statusCode
	currentInfo.ContentType = contentType
	currentInfo.ProcessedAt = time.Now()

	// --- Handle Download Outcomes ---

	// 1. Hard Error during download
	if downloadErr != nil && !(statusCode >= 300 && statusCode <= 399) {
		log.Printf("Worker %d: Download Error for asset/direct %s: %v", workerID, urlStr, downloadErr)
		currentInfo.Error = downloadErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: downloadErr})
		return
	}

	// 2. Redirect
	if statusCode >= 300 && statusCode <= 399 {
		redirectTargetURL := resultPath // downloadResource returns target URL on redirect
		log.Printf("Worker %d: Redirect detected for asset/direct %s -> %s", workerID, urlStr, redirectTargetURL)
		currentInfo.Error = fmt.Sprintf("Redirected (%d)", statusCode)
		currentInfo.Downloaded = false
		currentInfo.LocalPath = redirectTargetURL // Store target URL
		currentInfo.Status = StatusRedirected
		sm.UpdateLinkFull(currentInfo) // Save final state for original URL
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusRedirected, target: redirectTargetURL, contentType: contentType})

		// Process the redirect target - IMPORTANT: Pass original IsAsset hint
		handleRedirectTarget(workerID, redirectTargetURL, currentInfo.IsAsset, baseDomainURL, sm, discQueue, dlQueue, wg, tui, quit)
		return
	}

	// 3. Non-OK, Non-Redirect Status
	if statusCode != http.StatusOK {
		err := fmt.Errorf("HTTP status %d", statusCode)
		if downloadErr == nil {
			downloadErr = err
		} // Use explicit status if no other download error
		log.Printf("Worker %d: Non-OK status for asset/direct %s: %d", workerID, urlStr, statusCode)
		currentInfo.Error = downloadErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		if resultPath != "" {
			os.Remove(resultPath)
		} // Clean up file
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: downloadErr, contentType: contentType})
		return
	}

	// --- 4. Success (200 OK) --- Check for Cloudflare ---
	file, err := os.Open(resultPath)
	if err != nil {
		log.Printf("Worker %d: Error opening downloaded asset file %s for Cloudflare check: %v", workerID, resultPath, err)
		currentInfo.Error = fmt.Errorf("error opening file for check: %w", err).Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusError, err: err, contentType: contentType})
		return
	}
	bufReader := bufio.NewReader(file)
	isBlocked, checkErr, _ := isCloudflareChallenge(bufReader)
	file.Close()

	if checkErr != nil && checkErr != io.EOF && checkErr != io.ErrUnexpectedEOF {
		log.Printf("Worker %d: Error checking for Cloudflare challenge on asset/direct %s: %v. Proceeding cautiously.", workerID, urlStr, checkErr)
	}

	if isBlocked {
		log.Printf("Worker %d: Detected Cloudflare challenge page for asset/direct %s. Skipping.", workerID, urlStr)
		blockErr := errors.New("blocked by Cloudflare challenge")
		currentInfo.Error = blockErr.Error()
		currentInfo.Downloaded = false
		currentInfo.LocalPath = ""
		os.Remove(resultPath) // Delete the downloaded challenge page
		currentInfo.Status = StatusBlocked
		sm.UpdateLinkFull(currentInfo)
		tui.Send(urlUpdateMsg{url: urlStr, status: StatusBlocked, err: blockErr})
		return // <<< EXIT HERE for blocked assets
	}

	// --- 5. Success (200 OK), NOT Blocked - Finalize Asset ---
	log.Printf("Worker %d: Successfully downloaded asset/direct %s to %s", workerID, urlStr, resultPath)
	currentInfo.Downloaded = true
	currentInfo.Error = ""
	currentInfo.LocalPath = resultPath // Store the actual file path

	// Update IsAsset based on final content type.
	if !strings.HasPrefix(strings.ToLower(contentType), "text/html") {
		currentInfo.IsAsset = true
	} else {
		currentInfo.IsAsset = false // Explicitly mark as not asset if HTML (though unlikely for this worker)
	}

	// --- Final Update for Successful Download ---
	currentInfo.Status = StatusDownloaded
	sm.UpdateLinkFull(currentInfo) // Save the final successful state
	tui.Send(urlUpdateMsg{url: urlStr, status: StatusDownloaded, contentType: contentType})
	log.Printf("Worker %d: Completed processing download %s", workerID, urlStr)

	// **NO PARSING HERE** - This is the key difference for download workers.
}

// --- handleRedirectTarget (Helper function for queuing redirects) ---
func handleRedirectTarget(workerID int, targetURL string, isAssetHint bool, baseDomainURL *url.URL, sm *StateManager, discQueue chan string, dlQueue chan string, wg *sync.WaitGroup, tui *tea.Program, quit <-chan struct{}) {
	parsedTarget, err := url.Parse(targetURL)
	if err != nil {
		log.Printf("Worker %d: Failed to parse redirect target URL '%s': %v", workerID, targetURL, err)
		return
	}
	parsedTarget.Fragment = "" // Normalize
	normalizedTargetStr := parsedTarget.String()

	if isInternal(parsedTarget, baseDomainURL) {
		log.Printf("Worker %d: Redirect target '%s' is internal.", workerID, normalizedTargetStr)
		targetInfo, added := sm.AddLink(normalizedTargetStr, isAssetHint) // Add using original hint
		needsQ := sm.NeedsProcessing(normalizedTargetStr)

		if added {
			tui.Send(urlDiscoveredMsg{url: normalizedTargetStr, isAsset: targetInfo.IsAsset})
		}

		if needsQ {
			// Check status again before queueing
			linkInfo, exists := sm.GetLinkInfo(normalizedTargetStr)
			if exists && linkInfo.Status < StatusQueuedDiscovery { // Only queue if not already active
				wg.Add(1) // Increment *before* queueing attempt
				// Use the IsAsset hint from the *original* link that redirected (targetInfo.IsAsset)
				if targetInfo.IsAsset {
					log.Printf("Worker %d: Queueing redirect target (as asset) for download: %s", workerID, normalizedTargetStr)
					sm.UpdateLinkStatus(normalizedTargetStr, StatusQueuedDownload)
					tui.Send(urlUpdateMsg{url: normalizedTargetStr, status: StatusQueuedDownload})
					select {
					case dlQueue <- normalizedTargetStr: // Queue asset to download queue
					case <-quit:
						log.Printf("Worker %d: Quit signal while queueing redirect asset target %s", workerID, normalizedTargetStr)
						wg.Done() // Decrement if not queued
						return
					}
				} else {
					log.Printf("Worker %d: Queueing redirect target (as page) for discovery: %s", workerID, normalizedTargetStr)
					sm.UpdateLinkStatus(normalizedTargetStr, StatusQueuedDiscovery)
					tui.Send(urlUpdateMsg{url: normalizedTargetStr, status: StatusQueuedDiscovery})
					select {
					case discQueue <- normalizedTargetStr: // Queue non-asset (likely HTML) to discovery queue
					case <-quit:
						log.Printf("Worker %d: Quit signal while queueing redirect page target %s", workerID, normalizedTargetStr)
						wg.Done() // Decrement if not queued
						return
					}
				}
			} else {
				// log.Printf("Worker %d: Redirect target %s already queued or processing, skipping.", workerID, normalizedTargetStr)
			}
		} else {
			// log.Printf("Worker %d: Redirect target %s does not need processing.", workerID, normalizedTargetStr)
		}
	} else {
		log.Printf("Worker %d: Redirect target '%s' is external, ignoring.", workerID, normalizedTargetStr)
	}
}

// --- downloadResource (Unchanged - Returns redirect target in path field on 3xx) ---
func downloadResource(urlStr string, baseHost string) (resultPathOrRedirectTarget string, contentType string, statusCode int, err error) {
	client := &http.Client{
		Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Stop redirects, handle them manually
		},
	}

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", "", 0, fmt.Errorf("creating request failed: %w", err)
	}
	req.Header.Set("User-Agent", "GoWebCrawler/1.2 (Worker; +https://example.com/botinfo)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", 0, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	contentType = resp.Header.Get("Content-Type")
	finalURL := resp.Request.URL

	// Handle Redirects (3xx)
	if statusCode >= 300 && statusCode <= 399 {
		locationHeader := resp.Header.Get("Location")
		if locationHeader == "" {
			return "", contentType, statusCode, fmt.Errorf("redirect status %d with no Location header", statusCode)
		}
		redirectURL, err := url.Parse(locationHeader)
		if err != nil {
			return "", contentType, statusCode, fmt.Errorf("failed to parse Location header '%s': %w", locationHeader, err)
		}
		absoluteRedirectURL := finalURL.ResolveReference(redirectURL)
		// Return the absolute redirect URL string instead of a local path
		return absoluteRedirectURL.String(), contentType, statusCode, nil // Error is nil for redirect itself
	}

	// Handle Non-OK Statuses (Not 200, Not 3xx)
	if statusCode != http.StatusOK {
		// Read a small part of the body for potential error messages? Maybe later.
		bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 512)) // Read up to 512 bytes
		errMsg := fmt.Sprintf("HTTP status %d", statusCode)
		if readErr == nil && len(bodyBytes) > 0 {
			errMsg = fmt.Sprintf("%s - %s", errMsg, string(bodyBytes)) // Include snippet in error
		}
		return "", contentType, statusCode, errors.New(errMsg)
	}

	// --- Handle Success (200 OK) ---
	localPath := getFilePath(finalURL, contentType)
	if localPath == "" {
		return "", contentType, statusCode, fmt.Errorf("could not generate valid file path for %s", finalURL.String())
	}
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", contentType, statusCode, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	file, err := os.Create(localPath)
	if err != nil {
		return localPath, contentType, statusCode, fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		os.Remove(localPath)
		return localPath, contentType, statusCode, fmt.Errorf("failed to write file %s: %w", localPath, err)
	}
	// Success! Return the path, content type, status, and nil error
	return localPath, contentType, statusCode, nil
}

// --- getFilePath, guessExtension, sanitizePath, generateFilenameFromUrl (Unchanged from previous fixes) ---
func getFilePath(u *url.URL, contentType string) string {
	hostDir := strings.ReplaceAll(u.Host, ":", "_")
	path := filepath.Join(downloadBaseDir, hostDir)
	cleanedPath := strings.TrimPrefix(u.Path, "/")
	var finalPath string

	if cleanedPath == "" || cleanedPath == "." {
		if strings.HasPrefix(contentType, "text/html") {
			finalPath = filepath.Join(path, "index.html")
		} else {
			finalPath = filepath.Join(path, generateFilenameFromUrl(u, contentType))
		}
	} else {
		dirPart := filepath.Dir(cleanedPath)
		filePart := filepath.Base(cleanedPath)
		if dirPart == "." {
			path = filepath.Join(path, filePart)
		} else {
			path = filepath.Join(path, dirPart, filePart)
		}
		currentExt := filepath.Ext(path)
		hasTrailingSlash := strings.HasSuffix(u.Path, "/")
		if hasTrailingSlash {
			if strings.HasPrefix(contentType, "text/html") {
				finalPath = filepath.Join(path, "index.html")
			} else {
				finalPath = filepath.Join(path, generateFilenameFromUrl(u, contentType))
			}
		} else if currentExt == "" {
			ext := guessExtension(contentType)
			if ext != "" {
				finalPath = path + ext
			} else if strings.HasPrefix(contentType, "text/html") {
				finalPath = path + ".html"
			} else {
				dirOfPath := filepath.Dir(path)
				generatedName := generateFilenameFromUrl(u, contentType)
				finalPath = filepath.Join(dirOfPath, generatedName)
			}
		} else {
			finalPath = path
		}
	}
	return sanitizePath(finalPath)
}
func guessExtension(contentType string) string {
	contentType = strings.ToLower(strings.Split(contentType, ";")[0])
	switch contentType {
	case "text/html":
		return ".html"
	case "text/css":
		return ".css"
	case "application/javascript", "text/javascript":
		return ".js"
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/svg+xml":
		return ".svg"
	case "image/x-icon", "image/vnd.microsoft.icon":
		return ".ico"
	case "application/json":
		return ".json"
	case "application/xml", "text/xml":
		return ".xml"
	case "application/pdf":
		return ".pdf"
	case "font/woff":
		return ".woff"
	case "font/woff2":
		return ".woff2"
	case "font/ttf":
		return ".ttf"
	case "font/otf":
		return ".otf"
	case "font/eot":
		return ".eot"
	case "audio/mpeg":
		return ".mp3"
	case "audio/ogg":
		return ".ogg"
	case "audio/wav":
		return ".wav"
	case "audio/aac":
		return ".aac"
	case "audio/webm":
		return ".webm"
	case "video/mp4":
		return ".mp4"
	case "video/webm":
		return ".webm"
	case "video/ogg":
		return ".ogv"
	case "application/octet-stream":
		return ".bin"
	}
	if strings.HasPrefix(contentType, "audio/") {
		return ".audio"
	}
	if strings.HasPrefix(contentType, "video/") {
		return ".video"
	}
	if strings.HasPrefix(contentType, "image/") {
		return ".image"
	}
	if strings.HasPrefix(contentType, "font/") {
		return ".font"
	}
	return ""
}
func sanitizePath(path string) string {
	replacements := map[string]string{
		"<": "_lt_", ">": "_gt_", ":": "_colon_", "\"": "_quote_",
		"|": "_pipe_", "?": "_query_", "*": "_star_",
	}
	path = strings.ReplaceAll(path, "\\", string(filepath.Separator))
	path = strings.ReplaceAll(path, "/", string(filepath.Separator))
	for invalid, replacement := range replacements {
		if invalid != string(filepath.Separator) {
			path = strings.ReplaceAll(path, invalid, replacement)
		}
	}
	var sanitizedRunes []rune
	for _, r := range path {
		if r >= 0 && r <= 31 || r == 127 {
			sanitizedRunes = append(sanitizedRunes, '_')
		} else {
			sanitizedRunes = append(sanitizedRunes, r)
		}
	}
	path = string(sanitizedRunes)
	parts := strings.Split(path, string(filepath.Separator))
	validParts := []string{}
	maxLen := 240
	for i, part := range parts {
		if part == "" && i != 0 && len(parts) > 1 {
			continue
		}
		trimmedPart := strings.TrimRight(part, ". ")
		reservedNames := map[string]bool{
			"CON": true, "PRN": true, "AUX": true, "NUL": true, "COM1": true, "COM2": true,
			"COM3": true, "COM4": true, "COM5": true, "COM6": true, "COM7": true, "COM8": true,
			"COM9": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true,
			"LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true,
		}
		if _, isReserved := reservedNames[strings.ToUpper(trimmedPart)]; isReserved {
			trimmedPart = "_" + trimmedPart + "_"
		}
		if len(trimmedPart) > maxLen {
			ext := filepath.Ext(trimmedPart)
			base := trimmedPart[:len(trimmedPart)-len(ext)]
			if len(base) > maxLen-len(ext)-1 {
				base = base[:maxLen-len(ext)-1]
			}
			trimmedPart = base + ext
			if trimmedPart == "" || trimmedPart == "." || trimmedPart == ".." {
				trimmedPart = "__truncated__"
			}
		}
		if trimmedPart != "" || (i == 0 && part == "") {
			validParts = append(validParts, trimmedPart)
		} else if part != "" {
			validParts = append(validParts, "__sanitized__")
		}
	}
	finalPath := strings.Join(validParts, string(filepath.Separator))
	return finalPath
}
func generateFilenameFromUrl(u *url.URL, contentType string) string {
	base := filepath.Base(u.Path)
	name := ""
	if base != "." && base != "/" && base != "" && !strings.HasSuffix(u.Path, "/") {
		if filepath.Ext(base) != "" {
			name = base
		}
	}
	if name == "" && u.RawQuery != "" {
		escapedQ := url.QueryEscape(u.RawQuery)
		maxQueryLen := 50
		if len(escapedQ) > maxQueryLen {
			escapedQ = escapedQ[:maxQueryLen]
		}
		name = "query_" + escapedQ
	}
	if name == "" {
		hashInput := u.Path
		if u.RawQuery != "" {
			hashInput += "?" + u.RawQuery
		}
		if hashInput == "" || hashInput == "/" {
			name = fmt.Sprintf("resource_%x", time.Now().UnixNano())
		} else {
			h := 0
			for _, c := range hashInput {
				h = 31*h + int(c)
			}
			name = fmt.Sprintf("resource_%x", h)
		}
	}
	name = sanitizePath(name)
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	if name == "" {
		name = fmt.Sprintf("fallback_%x", time.Now().UnixNano())
	}
	ext := guessExtension(contentType)
	if ext == "" {
		if filepath.Ext(name) == "" {
			ext = ".unknown"
		}
	}
	maxFileLen := 240
	if len(name+ext) > maxFileLen {
		nameLen := maxFileLen - len(ext)
		if nameLen < 1 {
			nameLen = 1
		}
		name = name[:nameLen]
	}
	return name + ext
}

// --- FoundLink, findResourceLinks, getAttr, resolveURL, isInternal (Unchanged Structurally) ---
type FoundLink struct {
	URL     string
	IsAsset bool
}

func findResourceLinks(body io.Reader, pageURL *url.URL) []FoundLink {
	var links []FoundLink
	tokenizer := html.NewTokenizer(body)
	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			err := tokenizer.Err()
			if err != io.EOF {
				log.Printf("HTML Tokenizer Error on page %s: %v", pageURL.String(), err)
			}
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			linkURL := ""
			isAsset := false
			switch token.Data {
			case "a":
				linkURL = getAttr(token, "href")
				isAsset = false
			case "link":
				rel := strings.ToLower(getAttr(token, "rel"))
				href := getAttr(token, "href")
				if href != "" {
					switch rel {
					case "stylesheet", "icon", "apple-touch-icon", "manifest", "preload", "prefetch":
						linkURL = href
						isAsset = true
					case "canonical", "alternate", "prev", "next", "dns-prefetch", "preconnect":
						linkURL = href
						isAsset = false
					}
				}
			case "script":
				linkURL = getAttr(token, "src")
				if linkURL != "" {
					isAsset = true
				}
			case "img":
				linkURL = getAttr(token, "src")
				srcset := getAttr(token, "srcset")
				if linkURL != "" {
					isAsset = true
				}
				if srcset != "" {
					isAsset = true
					candidates := strings.Split(srcset, ",")
					for _, candidate := range candidates {
						parts := strings.Fields(strings.TrimSpace(candidate))
						if len(parts) > 0 {
							if trimmedSrc := strings.TrimSpace(parts[0]); trimmedSrc != "" {
								links = append(links, FoundLink{URL: trimmedSrc, IsAsset: true})
							}
						}
					}
				}
			case "source":
				linkURL = getAttr(token, "src")
				srcset := getAttr(token, "srcset")
				if linkURL != "" {
					isAsset = true
				}
				if srcset != "" {
					isAsset = true
					candidates := strings.Split(srcset, ",")
					for _, candidate := range candidates {
						parts := strings.Fields(strings.TrimSpace(candidate))
						if len(parts) > 0 {
							if trimmedSrc := strings.TrimSpace(parts[0]); trimmedSrc != "" {
								links = append(links, FoundLink{URL: trimmedSrc, IsAsset: true})
							}
						}
					}
				}
			case "audio", "video", "track":
				linkURL = getAttr(token, "src")
				if linkURL != "" {
					isAsset = true
				}
			case "iframe":
				linkURL = getAttr(token, "src")
				if linkURL != "" {
					isAsset = false
				}
			case "embed":
				linkURL = getAttr(token, "src")
				if linkURL != "" {
					isAsset = true
				}
			case "object":
				linkURL = getAttr(token, "data")
				if linkURL != "" {
					isAsset = true
				}
			}
			if linkURL != "" {
				if trimmedURL := strings.TrimSpace(linkURL); trimmedURL != "" {
					links = append(links, FoundLink{URL: trimmedURL, IsAsset: isAsset})
				}
			}
		}
	}
}
func getAttr(token html.Token, key string) string {
	for _, attr := range token.Attr {
		if attr.Key == key {
			return attr.Val
		}
	}
	return ""
}
func resolveURL(base *url.URL, relative string) *url.URL {
	if base == nil || relative == "" {
		return nil
	}
	relative = strings.TrimSpace(relative)
	lowerRelative := strings.ToLower(relative)
	if strings.HasPrefix(lowerRelative, "#") || strings.HasPrefix(lowerRelative, "mailto:") ||
		strings.HasPrefix(lowerRelative, "tel:") || strings.HasPrefix(lowerRelative, "javascript:") ||
		strings.HasPrefix(lowerRelative, "data:") || strings.HasPrefix(lowerRelative, "blob:") ||
		strings.HasPrefix(lowerRelative, "ftp:") || strings.HasPrefix(lowerRelative, "ws:") ||
		strings.HasPrefix(lowerRelative, "wss:") {
		return nil
	}
	relURL, err := url.Parse(relative)
	if err != nil {
		return nil
	}
	absoluteURL := base.ResolveReference(relURL)
	if absoluteURL.Scheme != "http" && absoluteURL.Scheme != "https" {
		return nil
	}
	absoluteURL.Fragment = ""
	return absoluteURL
}
func isInternal(linkURL *url.URL, baseDomainURL *url.URL) bool {
	if linkURL == nil || baseDomainURL == nil {
		return false
	}
	return linkURL.Host == baseDomainURL.Host
}
