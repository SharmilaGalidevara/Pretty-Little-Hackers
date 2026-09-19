from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    print("\n==============================")
    print("WEBHOOK RECEIVED!")
    print(data)
    print("==============================\n")

    return jsonify({"status": "received"}), 200


if __name__ == "__main__":
    print("Webhook server running!")
    print("Listening at http://127.0.0.1:8000/webhook")

    app.run(host="0.0.0.0", port=8000)