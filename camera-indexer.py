import cv2

def find_cameras():
    """
    Detects all available webcams and prints their indices.
    
    This function iterates through a range of common video device indices
    (from 0 to 9) and checks if a camera can be opened successfully.
    """
    print("Searching for connected cameras...")
    available_cameras = []
    # A common range to check for connected cameras
    for i in range(10):
        # cv2.VideoCapture(i) attempts to connect to the camera at index 'i'
        cap = cv2.VideoCapture(i)
        
        # Check if the camera was opened successfully
        if cap.isOpened():
            print(f"Camera found at index: {i}")
            available_cameras.append(i)
            # Release the camera to free the resource for other applications
            cap.release()
        else:
            # If the camera couldn't be opened, it means there's no camera at this index
            print(f"No camera found at index: {i}")

    if available_cameras:
        print("\n--- Summary ---")
        print("The following camera indices were found:")
        for index in available_cameras:
            print(f"- Index: {index}")
        print("\nUse one of these indices (e.g., 0, 1, etc.) in your 'webcam_producer.py' file to select a camera.")
    else:
        print("\n--- No Cameras Found ---")
        print("Please ensure your webcam is connected and the necessary drivers are installed.")

if __name__ == "__main__":
    find_cameras()
