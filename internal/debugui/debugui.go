package debugui

import _ "embed"

// Page is the dashboard, served as text/html.
//
//go:embed index.html
var Page []byte
