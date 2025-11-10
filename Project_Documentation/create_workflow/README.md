## Notes when creating the workflow

1. Delete any existing workflows and associated triggers if they exist
2. Create the workflow: run the script

```bash
create_workflow.sh
```

This will create the workflow, but it will not show up in the console yet.

3. Create the triggers and attach them to the workflow: run the script

```bash
create_triggers.sh
```

This will create the triggers and attach them to the workflow.  You will not see the workflow OR the triggers in the AWS Console yet though.

4. Create the starting point trigger - the first job that will be run in the workflow.  
**NOTE:** The first time you manually run the workflow, you'll need to run

```bash
create_on_demand_trigger.sh
```

This will create a trigger than can be executed on demand, which will be the first time we run the workflow to validate it.   

Once the workflow is validated (by running the rest of the steps below), you'll need to circle back to run the following script

```bash
create_daily_trigger.sh
```

This will delete the on_demand starting trigger and add the daily, scheduled trigger.

5. Confirm the state of the triggers before we force activation of the triggers.  Run the first script below.  If the trigger states come back wtih "State": "ACTIVATED", then you can skip the next script.  Otherwise, run the next script

```bash
show_trigger_state.sh

activate_triggers_manually.sh
```

6. Confirm the trigger states by running the script
```bash
show_trigger_state.sh
```

7. Manually run the workflow for the first time to "kickstart" the workflow engine.  It is only after you try to run the workflow for the first time that you will see the workflow and the triggers show up in the AWS Console.  
Manually run the workflow by running the script

```bash
test_workflow.sh
```

**Files in this folder:**  
README.md  
create_daily_trigger.sh  
create_triggers.sh  
create_workflow.sh  
show_workflow.sh  
status_workflow.sh  
test_workflow.sh  
trigger_bronze_to_metadata_silver.json  
trigger_daily_start.json  
trigger_metadata_to_fact_silver.json  
trigger_silver_to_gold.json  
verify_workflow.sh  
wistia_daily_workflow.json  

