# embucketd

The main executable (daemon) for the Embucket application, responsible for initializing and orchestrating all the different API services (Iceberg, Snowflake, UI, internal) and core components.

## Overview

This crate contains the `main` function for the Embucket server. It sets up logging, configuration, and starts the various Axum-based web services that provide Embucket's functionality.
