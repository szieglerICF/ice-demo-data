import json
from uuid import uuid4

from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

# Initialize the client
# project_client = AIProjectClient(
#     endpoint="https://53482-m1zet3ad-eastus2.cognitiveservices.azure.com/",
#     subscription_id="ad6b68df-0f76-427c-af5f-0a3033e93d2d",
#     resource_group_name= "rg-aif-dmx1d3625a8",
#     project_name="DMX AI Project",
#     credential=DefaultAzureCredential()
# )


project_client = AIProjectClient.from_connection_string(
    conn_str="eastus.api.azureml.ms;ad6b68df-0f76-427c-af5f-0a3033e93d2d;rg-aif-dmx1d3625a8;AIF-dmx-ai-project799c9fc0",
    credential=DefaultAzureCredential()
)


thread = project_client.agents.create_thread()
print(f"Thread ID: {thread.id}")


# # Interact with your agent
# run = project_client.agents.create_run(
#     thread_id=thread.id,
#     assistant_id="asst_P2vaEMvvlQ6FP3NKb9P27N0f",
# )

agent_id = "asst_P2vaEMvvlQ6FP3NKb9P27N0f"


message = project_client.agents.create_message(thread_id=thread.id, role="user", content="What color is a banana?")

run = project_client.agents.create_run(thread_id=thread.id, assistant_id=agent_id)

import time

# Poll the run as long as run status is queued or in progress
while run.status in ["queued", "in_progress", "requires_action"]:
    # Wait for a second
    time.sleep(1)
    run = project_client.agents.get_run(thread_id=thread.id, run_id=run.id)
    print(run)


# Get the messages in the thread
messages = project_client.agents.list_messages(thread_id=thread.id)

# The messages are following in the reverse order,
# we will iterate them and output only text contents.
for data_point in reversed(messages.data):
    last_message_content = data_point.content[-1]
    # if isinstance(last_message_content, MessageTextContent):
    print(f"{data_point.role}: {last_message_content.text.value}")






