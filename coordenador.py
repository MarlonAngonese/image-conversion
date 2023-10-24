from flask import Flask, request, jsonify, send_file
import pika
from io import BytesIO

app = Flask(__name__)

@app.route('/convert', methods=['POST'])
def convert():
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        image_data = file.read()

        print("Received image of size:", len(image_data))
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Envia imagem para o processador
        channel.queue_declare(queue='image_queue')
        channel.basic_publish(
            exchange='',
            routing_key='image_queue',
            body=image_data
        )

        #Aguarda a resposta do processador
        channel.queue_declare(queue='response_queue')
        method, properties, body = channel.basic_get(
            queue='response_queue'
        )

        if (body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return send_file(BytesIO(body), mimetype='image/jpeg')
        else:
            return jsonify({'error': 'Image processing delay or error.', 'body': body}), 500
        
        connection.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)