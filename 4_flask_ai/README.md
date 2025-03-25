# Serve a model with Flask

Based on this [pathology mobile web app project](https://www.omdena.com/blog/pathology-mobile-app).

The idea is to deploy a segmentation model saved as an ONNX file with Flask and Docker / Docker compose, so that a user can upload an image and get the
segmented result (e.g. bounding boxes around people in the image).

## How to

- Download the model into the `data` folder: [Mask R-CNN model](https://github.com/onnx/models/blob/main/validated/vision/object_detection_segmentation/mask-rcnn/model/MaskRCNN-12-qdq.onnx)
- Run with Docker:

    ```bash
    docker compose up
    ```

- Go to `127.0.0.1:5000` and upload an image (.png, .jpeg or .jpg)
