"""Run this to find which camera index works: python find_camera.py"""
import cv2

for i in range(6):
    cap = cv2.VideoCapture(i)
    if cap.isOpened():
        ret, _ = cap.read()
        print(f"Camera {i}: {'OK — can read frames' if ret else 'opened but read() failed'}")
        cap.release()
    else:
        print(f"Camera {i}: not found")
