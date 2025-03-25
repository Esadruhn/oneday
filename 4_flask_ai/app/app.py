import pathlib
import math
import cv2

import numpy as np
import onnxruntime as rt
import matplotlib.pyplot as plt
import matplotlib.patches as patches

from PIL import Image, ImageDraw
from io import BytesIO
from flask import Flask, flash, request, redirect, send_file, Response


MODEL_PATH = pathlib.Path(__file__).parent.resolve() / "data" / "MaskRCNN-12-qdq.onnx"
CLASSES_PATH = pathlib.Path(__file__).parent.resolve() / "data" / "coco_classes.txt"
OUTPUT_PATH = pathlib.Path(__file__).parent.resolve() / "data" / "output.png"

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg"}

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1000 * 1000


def allowed_file(filename: str):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def preprocess(image):
    # Resize
    ratio = 800.0 / min(image.size[0], image.size[1])
    image = image.resize(
        (int(ratio * image.size[0]), int(ratio * image.size[1])), Image.BILINEAR
    )

    # Convert to BGR
    image = np.array(image)[:, :, [2, 1, 0]].astype("float32")

    # HWC -> CHW
    image = np.transpose(image, [2, 0, 1])

    # Normalize
    mean_vec = np.array([102.9801, 115.9465, 122.7717])
    for i in range(image.shape[0]):
        image[i, :, :] = image[i, :, :] - mean_vec[i]

    # Pad to be divisible of 32

    padded_h = int(math.ceil(image.shape[1] / 32) * 32)
    padded_w = int(math.ceil(image.shape[2] / 32) * 32)

    padded_image = np.zeros((3, padded_h, padded_w), dtype=np.float32)
    padded_image[:, : image.shape[1], : image.shape[2]] = image
    image = padded_image

    return image


def get_output_img(image, boxes, labels, scores, masks, file, score_threshold=0.7):
    classes = [line.rstrip("\n") for line in CLASSES_PATH.open("r")]

    # Resize boxes
    ratio = 800.0 / min(image.size[0], image.size[1])
    boxes /= ratio

    _, ax = plt.subplots(1, figsize=(12, 9))

    image = np.array(image)

    for mask, box, label, score in zip(masks, boxes, labels, scores):
        # Showing boxes with score > 0.7
        if score <= score_threshold:
            continue

        # Finding contour based on mask
        mask = mask[0, :, :, None]
        int_box = [int(i) for i in box]
        mask = cv2.resize(
            mask, (int_box[2] - int_box[0] + 1, int_box[3] - int_box[1] + 1)
        )
        mask = mask > 0.5
        im_mask = np.zeros((image.shape[0], image.shape[1]), dtype=np.uint8)
        x_0 = max(int_box[0], 0)
        x_1 = min(int_box[2] + 1, image.shape[1])
        y_0 = max(int_box[1], 0)
        y_1 = min(int_box[3] + 1, image.shape[0])
        mask_y_0 = max(y_0 - box[1], 0)
        mask_y_1 = mask_y_0 + y_1 - y_0
        mask_x_0 = max(x_0 - box[0], 0)
        mask_x_1 = mask_x_0 + x_1 - x_0
        im_mask[y_0:y_1, x_0:x_1] = mask[mask_y_0:mask_y_1, mask_x_0:mask_x_1]
        im_mask = im_mask[:, :, None]

        # OpenCV version 4.x
        contours, hierarchy = cv2.findContours(
            im_mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE
        )

        image = cv2.drawContours(image, contours, -1, 25, 3)

        rect = patches.Rectangle(
            (box[0], box[1]),
            box[2] - box[0],
            box[3] - box[1],
            linewidth=1,
            edgecolor="b",
            facecolor="none",
        )
        ax.annotate(
            classes[label] + ":" + str(np.round(score, 2)),
            (box[0], box[1]),
            color="w",
            fontsize=12,
        )
        ax.add_patch(rect)

    ax.imshow(image)
    plt.savefig(file)


def predict(file):
    model = rt.InferenceSession(MODEL_PATH)
    pil_img = Image.open(file)
    img_data = preprocess(pil_img)

    boxes, labels, scores, masks = model.run(None, {"image": img_data})
    get_output_img(
        image=pil_img,
        boxes=boxes,
        labels=labels,
        scores=scores,
        masks=masks,
        file=OUTPUT_PATH,
    )
    return send_file(OUTPUT_PATH.as_posix(), mimetype="image/png")


@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        # check if the post request has the file part
        if "file" not in request.files:
            flash("No file part")
            return redirect(request.url)
        file = request.files["file"]
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == "":
            flash("No selected file")
            return redirect(request.url)
        if file and allowed_file(file.filename):
            response = predict(file=file)
            return response, 200
    return """
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    """


@app.route("/favicon.ico")
def favicon():
    # Create a blank image with an alpha channel
    image = Image.new("RGBA", (100, 100), (255, 255, 255, 0))

    outer_radius = 25
    inner_radius = 15
    center_x = 50
    center_y = 50
    draw = ImageDraw.Draw(image)
    draw.ellipse(
        (
            center_x - outer_radius,
            center_y - outer_radius,
            center_x + outer_radius,
            center_y + outer_radius,
        ),
        fill="#4682b4",
    )
    draw.ellipse(
        (
            center_x - inner_radius,
            center_y - inner_radius,
            center_x + inner_radius,
            center_y + inner_radius,
        ),
        fill="#fff",
    )

    image_version = image.resize((40, 40), resample=Image.BICUBIC)

    buffer = BytesIO()
    image_version.save(buffer, "ICO")
    buffer.seek(0)
    return Response(
        buffer, mimetype="image/vnd.microsoft.icon", direct_passthrough=True
    )


if __name__ == "__main__":
    app.run()
