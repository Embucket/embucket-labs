# api-ui-static-assets

This crate assumes that the file `ui/dist.tar` already exists in the project's root directory.

It provides static assets packaged as a JavaScript bundle, including CSS, JavaScript, and images for the Embucket UI. These assets are embedded into the application at compile time.

## Purpose

This crate includes the compiled frontend assets (e.g., from a JavaScript framework build) as a Rust library, allowing them to be served efficiently by the `api-ui` crate.
