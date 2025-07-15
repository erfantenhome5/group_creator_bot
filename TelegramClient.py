import random
from telethon import TelegramClient

# A list of possible device parameters to choose from, including a mix of mobile and desktop clients
# to make the "secure browser" option seem more plausible.
device_params = [
    # --- Mobile Clients ---
    {'device_model': 'iPhone 14 Pro Max', 'system_version': '16.5.1', 'app_version': '9.6.3'},
    {'device_model': 'Samsung Galaxy S23 Ultra', 'system_version': 'SDK 33', 'app_version': '9.6.3'},
    {'device_model': 'Google Pixel 7 Pro', 'system_version': 'SDK 33', 'app_version': '9.6.3'},
    {'device_model': 'iPhone 15 Pro', 'system_version': '17.1.1', 'app_version': '10.2.2'},
    {'device_model': 'Xiaomi 13 Ultra', 'system_version': 'SDK 34', 'app_version': '10.2.0'},

    # --- Desktop / Web Clients (to enhance the "browser" illusion) ---
    {'device_model': 'PC 64bit', 'system_version': 'Windows 10', 'app_version': '4.11.1'},
    {'device_model': 'MacBook Pro', 'system_version': 'macOS 14.0', 'app_version': '10.2.2'},
    {'device_model': 'PC 64bit', 'system_version': 'Windows 11', 'app_version': '4.14.5'},
    {'device_model': 'Linux', 'system_version': 'Ubuntu 22.04', 'app_version': '4.12.0'},
]

def create_client(session_file, api_id, api_hash):
    """
    Creates a TelegramClient instance with a randomly selected device profile.
    This function should be called when a new client object is needed.
    """
    # Randomly select a set of parameters for the new session
    selected_device = random.choice(device_params)
    
    print(f"Initializing client with profile: {selected_device['device_model']}")

    client = TelegramClient(
        session_file,
        api_id,
        api_hash,
        device_model=selected_device['device_model'],
        system_version=selected_device['system_version'],
        app_version=selected_device['app_version']
    )
    return client

# Note: The original file seemed to have top-level code to create a client.
# It's better practice to wrap this in a function as shown above (`create_client`)
# and call it from your main script when you need a client instance.
# The original code is commented out below for reference.

# selected_device = random.choice(device_params)
# client = TelegramClient(
#     session_file,
#     api_id,
#     api_hash,
#     device_model=selected_device['device_model'],
#     system_version=selected_device['system_version'],
#     app_version=selected_device['app_version']
# )
