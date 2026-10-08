import numpy as np
import sys
import os

# Add the parent directory to the path so we can import inference
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.inference import process_yolo_output
except ImportError:
    print("WARNING: Could not import process_yolo_output from app.inference. Using a dummy fallback for testing.")
    def process_yolo_output(output, conf_threshold=0.45, iou_threshold=0.45, orig_shape=(1080, 1920), new_shape=(640, 640)):
        # Implementation is in app.inference, this just tests NMS logic
        pass

def nms(boxes, scores, iou_threshold):
    if len(boxes) == 0:
        return []
    
    x1 = boxes[:, 0]
    y1 = boxes[:, 1]
    x2 = boxes[:, 2]
    y2 = boxes[:, 3]
    areas = (x2 - x1) * (y2 - y1)
    
    order = scores.argsort()[::-1]
    keep = []
    
    while order.size > 0:
        i = order[0]
        keep.append(i)
        
        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])
        
        w = np.maximum(0.0, xx2 - xx1)
        h = np.maximum(0.0, yy2 - yy1)
        inter = w * h
        
        iou = inter / (areas[i] + areas[order[1:]] - inter)
        
        inds = np.where(iou <= iou_threshold)[0]
        order = order[inds + 1]
        
    return keep

def test_nms():
    # Test case: 3 boxes, 2 are overlapping
    # Format: [x1, y1, x2, y2]
    boxes = np.array([
        [100, 100, 200, 200],  # Box 1 (Score 0.9)
        [110, 110, 190, 190],  # Box 2 (Score 0.8) - highly overlaps with Box 1
        [300, 300, 400, 400],  # Box 3 (Score 0.7) - independent
    ], dtype=np.float32)
    
    scores = np.array([0.9, 0.8, 0.7], dtype=np.float32)
    
    keep = nms(boxes, scores, iou_threshold=0.5)
    
    # We expect to keep Box 1 (index 0) and Box 3 (index 2)
    # Box 2 should be suppressed by Box 1
    expected = [0, 2]
    
    if keep == expected:
        print("SUCCESS: NMS Test Passed. Correct boxes were kept.")
    else:
        print(f"FAILED: NMS Test Failed. Expected {expected}, got {keep}")

if __name__ == "__main__":
    print("Running NMS test...")
    test_nms()
