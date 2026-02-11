# tap-branch

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [branch API](https://help.branch.io/apidocs/apis-overview).
- Extracts the following resources:
    - [AppConfig](https://help.branch.io/apidocs/getcurrentbrancappconfig)

    - [DeepLink](https://help.branch.io/apidocs/readexistingdeeplink)

    - [Custom Export Endpoints](https://help.branch.io/apidocs/custom-exports-api)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[app_config](https://help.branch.io/apidocs/getcurrentbrancappconfig)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[deeplink](https://help.branch.io/apidocs/readexistingdeeplink)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[branch_events](https://help.branch.io/apidocs/custom-exports-api)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-branch
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json

    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
    - `branch_app_id`: Your Branch App ID
    - `branch_access_token`: Your Branch Access Token
    - `branch_key`: Your Branch Key
    - `branch_secret`: Your Branch Secret
    - `deeplink_urls`: A string of comma seperated deeplink urls to extract data for
    - `start_date`: The start date for data extraction in ISO 8601 format

    ```json
    {
    "branch_app_id": "branch-app-id",
    "branch_access_token": "branch-access-token",
    "branch_key": "branch-key",
    "branch_secret": "branch-secret",
    "deeplink_urls": "deeplink-url-1, deeplink-url-2",
    "start_date": "2023-01-01T00:00:00Z"
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "templates",
        "bookmarks": {
            "logs": "2019-09-27T22:34:39Z",
            "snippets": "2019-09-28T15:30:26Z",
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-branch --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-branch --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-branch --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-branch --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    While developing the branch tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_branch -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-branch --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .
    ```
---

Copyright &copy; 2019 Stitch
