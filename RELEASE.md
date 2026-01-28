# Release Guide

This document explains how to create releases for the Pinot Go Client library using the simplified GitHub Actions workflow.

## Overview

The release process is streamlined and manual. You trigger the workflow with a tag input, and it will:

1. **Validate tag format** - Ensure semantic versioning format
2. **Check tag availability** - Verify the tag doesn't already exist
3. **Create and push tag** - Run `git tag` and `git push` commands
4. **Generate changelog** - Create release notes from commit history
5. **Publish release** - Create a GitHub release page

## How to Create a Release

### 1. Ensure your code is ready

Before creating a release, make sure:
- All changes are committed and pushed to `master`
- You've decided on the version number (e.g., v1.2.3)

### 2. Trigger the release workflow

1. Go to the **Actions** tab in your GitHub repository
2. Click on "Release" workflow
3. Click "Run workflow" button
4. Enter your desired tag (e.g., `v1.2.3`)
5. Click "Run workflow"

### 3. Workflow will handle everything

The workflow will automatically:
- Create the git tag
- Push the tag to remote
- Generate changelog from commit history
- Create a GitHub release page

### 4. Release is ready

Once the workflow completes successfully:
- A GitHub release will be created with changelog
- Go developers can install the new version with:
  ```bash
  go get github.com/startreedata/pinot-client-go@v1.2.3
  ```

## Tag Format Requirements

The workflow validates tag formats and only accepts:
- **Stable releases**: `v1.2.3`, `v2.0.0`, `v1.15.7`
- **Pre-releases**: `v1.2.3-alpha`, `v1.2.3-beta`, `v1.2.3-rc1`

Invalid examples:
- `1.2.3` (missing 'v' prefix)
- `v1.2` (missing patch version)
- `release-1.2.3` (wrong format)

## What Gets Included in a Release

### Automatic Release Notes
- Changelog generated from commit messages since the last release
- Installation instructions
- Usage examples
- Link to full changelog on GitHub

### Suggested Release Notes Template
If a release adds or changes broker behavior, include a short usage note so users can copy/paste.

Example snippet:
````markdown
## Usage (Broker)
Pinot brokers can be queried over gRPC when `pinot.broker.grpc.port` is enabled.

```go
pinotClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
    BrokerList: []string{"localhost:8010"},
    GrpcConfig: &pinot.GrpcConfig{
        Encoding:     "JSON",  // or "ARROW"
        Compression:  "ZSTD",
        BlockRowSize: 10000,
        Timeout:      5 * time.Second,
    },
})
```
````

### Major Version Tags
For stable releases, the workflow also updates major version tags (e.g., `v1` for `v1.2.3`) to allow users to get the latest version within a major version.

## Pre-releases

Pre-releases (tags containing `-alpha`, `-beta`, `-rc`, etc.) are:
- Marked as "Pre-release" in GitHub
- Not considered as the "Latest" release
- Suitable for testing and preview versions

## Troubleshooting

### Tag Already Exists
If you get an error that the tag already exists:
1. Choose a different version number, or
2. Delete the existing tag first:
   ```bash
   git tag -d v1.2.3
   git push origin :refs/tags/v1.2.3
   ```

### Release Workflow Failed
If the release workflow fails:
1. Check the workflow logs in the Actions tab
2. Common issues:
   - Invalid tag format (fix and re-run)
   - Tag already exists (use different version)
   - Network issues (retry the workflow)

## Manual Release (Alternative)

You can also create releases manually without the workflow:

1. Create and push tag locally:
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

2. Go to the **Releases** page in your GitHub repository
3. Click "Create a new release"
4. Select your tag
5. Fill in the release notes
6. Publish the release

However, the automated workflow is recommended for consistency.
