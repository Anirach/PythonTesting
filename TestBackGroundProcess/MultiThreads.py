import threading
import time

def background_function():
    print("This is running in the background.")
    print("Sleeping for 5 seconds...")
    time.sleep(5)
    print("done")

# Create a thread that targets the background function
background_thread = threading.Thread(target=background_function)

# Start the thread
background_thread.start()

# The main program continues to run while the background function executes
print("This is running in the main thread.")
