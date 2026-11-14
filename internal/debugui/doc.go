// Package debugui holds the page Cache.DebugHandler serves at /ui: one HTML
// file, embedded, reading the handler's own JSON routes from the browser. It
// has no dependencies and loads nothing from the network, so it works on an
// internal admin port with no internet access.
package debugui
