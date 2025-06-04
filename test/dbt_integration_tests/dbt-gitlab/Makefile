
# General Variables
DBT_REFERENCE= "curl https://dbt.gitlabdata.com/manifest.json -o reference_state/manifest.json"
GIT_BRANCH_BASE = $$(git merge-base --fork-point origin/master)
DBT_DEV_STATE=--state reference_state
DEV_LOCAL_CHANGES:=$(shell { git diff --name-only $(GIT_BRANCH_BASE) ; git ls-files --other --exclude-standard ; })
CI_CHANGES:=$(shell git diff --name-only $$CI_MERGE_REQUEST_DIFF_BASE_SHA)

## Command Variables
GIT_CHANGES_ROOT=$$(if [ "$(MODE)" = "CI" ] ; then echo "$(CI_CHANGES)" ; else echo "$(DEV_LOCAL_CHANGES)"; fi)
DBT_CONTIG_VARS=$$(if [ ! -z "$(SELECTION)" ]; then echo ""; else echo "\"DOWNSTREAM_LIST\":\"$(DBT_DEV_CHANGES_DOWNSTREAM)\",\"UPSTREAM_LIST\":\"$(DBT_DEV_CHANGES_UPSTREAM)\""; fi)
DBT_DEV_CHANGES=$(shell echo $(GIT_CHANGES_ROOT) | xargs -n1 | grep '\.sql\|\.csv\|models/.*\.py' | xargs -n1 basename | sed -E "s/(.sql|.csv|.py)//g" )
DBT_DEV_CHANGES_OPERATORS = $(addprefix $(UPSTREAM),$(addsuffix $(DOWNSTREAM),$(DBT_DEV_CHANGES)))
DBT_DEV_CHANGES_DOWNSTREAM=$(shell echo $(GIT_CHANGES_ROOT) \
| xargs -n1 \
| grep '\.sql\|\.csv\|models/.*\.py' \
| xargs -n1 basename \
| sed -E "s/(.sql|.csv|.py)/+/g" \
| tr '\n' ',' \
| sed "s/\(.*\),/\1/g")
DBT_DEV_CHANGES_UPSTREAM=$(shell echo $(GIT_CHANGES_ROOT) \
| xargs -n1 \
| grep '\.sql\|\.csv\|models/.*\.py' \
| xargs -n1 basename \
| sed "s/^/+/g" \
| sed -E "s/(.sql|.csv|.py)//g" \
| tr '\n' ',' \
| sed "s/\(.*\),/\1/g")


DBT_DEV_SELECTION=$$(if [ ! -z "$(SELECTION)" ] ; then echo "--select $(SELECTION)"; elif [ ! -z "$(CONTIGUOUS)" ] ; then echo "--select $(DBT_DEV_CHANGES_OPERATORS)"; else echo "--selector contiguous_list"; fi)
DBT_DEV_FAIL_FAST= $$(if [ -z "$(FAIL_FAST)" ]; then echo "--fail-fast"; fi)
DBT_DEV_EXCLUDE= $$(if [ ! -z "$(EXCLUDE)" ]; then echo "--exclude $(EXCLUDE)"; fi)
DBT_DEV_FULL_REFRESH=$$(if [ ! -z "$(FULL_REFRESH)" ]; then echo "--full-refresh"; fi)
VARS_QUOTE=$$(echo $(VARS) | xargs -n1 | sed -e "s/^/\"/g" -e "s/:/\":\"/g" -e "s/,/\",\"/g" -e "s/$$/\"/g")
DBT_DEV_VARS= $$( \
	if [ ! -z "$(VARS_QUOTE)" ] && [ -z "$(CONTIGUOUS)" ]; \
	then echo "--vars {$(VARS_QUOTE),$(DBT_CONTIG_VARS)}" ; \
	elif [ -z "$(CONTIGUOUS)" ]; \
	then echo "--vars {$(DBT_CONTIG_VARS)}"; \
	elif [ ! -z "$(VARS_QUOTE)" ]; \
	then echo "--vars {$(VARS_QUOTE)}"; fi \
	)
DBT_DEV_TARGET= $$(if [ ! -z "$(PROFILE_TARGET)" ] ; then echo "$(PROFILE_TARGET)"; elif [ ! -z "$(TARGET)" ] ; then echo "--target $(TARGET)"; fi )

SQLFLUFF_DEV_CHANGES=$(shell echo $(GIT_CHANGES_ROOT) | xargs -n1 | grep '\.sql\b' | cut -d'/' -f3- ) 
SQLFLUFF_DEV_MODEL_PATH=$$(find . -name "$(MODEL).sql")
SQLFLUFF_DEV_MODELS=$$(if [ -z "$(MODEL)" ]; then echo $(SQLFLUFF_DEV_CHANGES); else echo $(SQLFLUFF_DEV_MODEL_PATH); fi)
SQLFLUFF_DEV_COMMAND=$$(if [ -z "$(FIX)" ]; then echo "lint"; else echo "fix"; fi)
SQLFLUFF_DEV_CONFIG=$$(if [ "$(MODE)" = "CI" ] ; then echo "--config .sqlfluff-ci" ; else echo ""; fi)

## Commands
DBT_DEV_BUILD_CHANGES_CLONE_COMMAND= dbt $(DBT_DEV_FAIL_FAST) clone $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) $(DBT_DEV_STATE) $(DBT_DEV_VARS)
DBT_DEV_BUILD_CHANGES_BUILD_COMMAND= dbt $(DBT_DEV_FAIL_FAST) build $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) --defer $(DBT_DEV_STATE)  $(DBT_DEV_VARS)

DBT_DEV_SAFE_CHECK_COMMAND= dbt --quiet list --selector unsafe_models --resource-type model $(DBT_DEV_TARGET) --output json > safe_models.json 
DBT_UPSTREAM_CHECK_COMMAND = dbt --quiet list --select $(DBT_DEV_CHANGES_OPERATORS) $(DBT_DEV_TARGET) --resource-type=model --output json > upstream_models.json 


SQLFLUFF_COMMAND= sqlfluff $(SQLFLUFF_DEV_COMMAND) $(SQLFLUFF_DEV_MODELS) $(SQLFLUFF_DEV_CONFIG)


help:
	@echo "\n \
	------------------------------------------------------------ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ dbt Related ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - build-changes: will perform a clone and build of all of the changes sql and csv files accepts the following variables: \n \
		 - CONTIGUOUS - defaults to true, providing any value will set to false \n \
		 - DOWNSTREAM - dbt graph operator for including downstream models to the changes \n \
		 - SELECTION - overrides the list of changed models for the provided dbt selection \n \
		 - FAIL_FAST - defaults to true, providing any value will set to false \n \
		 - EXCLUDE - a dbt selection of models to not clone or build \n \
		 - FULL_REFRESH - defaults to false, providing any value will set to true \n \
		 - VARS - list of key value pairs to pass to dbt, must be in the format \"key\":\"value\" \n \
		 - TARGET - a dbt target to use that is not the default \n \
		 - MODE - defaults to DEV, providing the value of CI will adjust behavior to work within the CI environment  \n \
		 - PROFILE_TARGET - a combination of profile director and target, used in the CI jobs \n \
    - lint-models: will lint or fix all of the changed sql files or the provided model \n \
		 - MODEL: overrides the list of changed files for the provided file name \n \
		 - FIX: defaults to false, providing any value will set to true and make possible changes to files \n \
    - safe-check: will check for models downstream of SAFE data not properly tagged \n \
	- run-grants: runs grants to clone for selected user using varibale GRANT_TO_ROLE \n \
	 \n \
	------------------------------------------------------------"

build-changes:
	@if [ ! "$(MODE)" = "CI" ]; then (echo "\n***** Fetching Reference *******\n"; "$(DBT_REFERENCE)";) fi;

	@echo "\n***** User Input *******\n"
	@echo User Selection: $(SELECTION)
	@echo User Fail Fast: $(FAIL_FAST)
	@echo User Exclude: $(EXCLUDE)
	@echo User Full Refresh: $(FULL_REFRESH)
	@echo User Vars: $(VARS)
	@echo User Target: $(TARGET)
	@echo User Downstream: $(DOWNSTREAM)

	@echo "\n***** Resultant Values *******\n"
	@echo Selection: $(DBT_DEV_SELECTION)
	@echo Fail Fast: $(DBT_DEV_FAIL_FAST)
	@echo Exclude: $(DBT_DEV_EXCLUDE)
	@echo Full Refresh: $(DBT_DEV_FULL_REFRESH)
	@echo Vars: $(DBT_DEV_VARS)
	@echo Target: $(DBT_DEV_TARGET)

	@echo "\n***** Cloning Selection *******\n"
	@echo $(DBT_DEV_BUILD_CHANGES_CLONE_COMMAND)
	@$(DBT_DEV_BUILD_CHANGES_CLONE_COMMAND) || (echo "Error in Clone"; exit 4)
	@echo "\n***** Building Selection *******\n"
	@echo $(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) 
	@$(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) || (echo "Error in Build"; exit 5)

lint-models:
	@echo $(SQLFLUFF_COMMAND)
	@$(SQLFLUFF_COMMAND)

safe-check:
	@echo "\n***** Getting List of un-SAFE Models *******\n"
	@echo "$(DBT_DEV_SAFE_CHECK_COMMAND)"
	@$(DBT_DEV_SAFE_CHECK_COMMAND) || (echo "Error collecting non-SAFE list in dbt"; exit 1)
	@echo "\n***** Checking List of un-SAFE Models *******\n"
	@python3 dbt_ci_utils.py clean_up_json --file safe_models.json
	@python3 dbt_ci_utils.py check_safe_models --file safe_models.json

run-grants:
	@echo "\n***** Getting upstream *******\n"
	@echo "$(DBT_UPSTREAM_CHECK_COMMAND)"
	@$(DBT_UPSTREAM_CHECK_COMMAND) || (echo "Error collecting upstream list in dbt"; exit 1)
	@python3 dbt_ci_utils.py clean_up_json --file upstream_models.json
	@echo "\n***** Granting Access to Clones *******\n"
	@echo granting to roles: $(GRANT_TO_ROLES)
	@$(foreach role,$(GRANT_TO_ROLES), python3 ../../orchestration/manage_snowflake.py grant_clones --database prep --role $(role) --file upstream_models.json;)
	@$(foreach role,$(GRANT_TO_ROLES), python3 ../../orchestration/manage_snowflake.py grant_clones --database prod --role $(role) --file upstream_models.json;)

	
