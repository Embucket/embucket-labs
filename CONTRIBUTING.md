# Contributing
Embucket is a streamlined solution for building and managing an open lakehouse platform based on the Apache Iceberg open table format. This project is designed to be radically simple, focusing on ease of deployment, operation, and maintenance.


When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.
Please note we have a [code of conduct](CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

We appreciate every contribution, whether it's fixing bugs, improving documentation, or adding new features — your efforts help make this project better for everyone.


## Development environment setup

To set up a development environment, please follow these steps:

1. Clone the embucket/embucket-labs repository

   ```sh
   git clone https://github.com/embucket/embucket-labs.git
   cd embucket/
   ```
   
2. Build the project
   ```sh
   cargo build
   ```


## Issues and feature requests

You've found a bug in the source code, a mistake in the documentation or maybe you'd like a new feature? You can help us by. Before you create an issue, make sure to search the issue archive  [GithubIssues](https://github.com/embucket/embucket-labs/issues?q=is%3Aissue) your issue may have already been addressed!

Please try to create bug reports that are:

- _Reproducible._ Include steps to reproduce the problem.
- _Specific._ Include as much detail as possible: which version, what environment, etc.
- _Unique._ Do not duplicate existing opened issues.
- _Scoped to a Single Bug._ One bug per report.

**Even better: Submit a pull request with a fix or new feature!**

### How to submit a Pull Request

1. Search our repository for open or closed
   [Pull Requests](https://github.com/embucket/embucket-labs/pulls)
   that relate to your submission. You don't want to duplicate effort.
2. Fork the project
3. Create your feature branch (`git checkout -b issueID/new_feature`)
4. Commit your changes (`git commit -m 'issue id: New Feature'`)
5. Push to the branch (`git push origin issueID/new_feature`)
6. [Open a Pull Request](https://github.com/embucket/embucket-labs/compare?expand=1)
7. Your pull request build must pass (the build will run automatically).


## Coding conventions

When coding in Rust, please follow the [coding conventions](CODING_CONVENTIONS.md).


## Getting Help
For discussions, questions, or collaboration, feel free to reach out via [GitHub Discussions](https://github.com/embucket/embucket-labs/discussions). We appreciate your contributions and look forward to working with you!
