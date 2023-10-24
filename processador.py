import pika
from PIL import Image
from io import BytesIO

def process_image_callback(ch, method, properties, body):
    try:
        # Converte iamgem PNG para JPG
        image = Image.open(BytesIO(body))

        print("The image format is", image.format)

        if image.format != 'PNG':
            print('NOT a PNG. Skipping...')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        image = image.convert("RGB")

        with BytesIO() as output:
            image.save(output, format="JPEG")
            converted_image_data = output.getvalue()

        channel.basic_publish(
            exchange='',
            routing_key='response_queue',
            body=converted_image_data
        )

        print('Converted PNG to JPG and sent back to coordinator!')
    except Exception as e:
        print('Received invalid image data. Skipping...', str(e))

    ch.basic_ack(delivery_tag=method.delivery_tag)

credentials = pika.PlainCredentials('processador', 'processador')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.20.0.1', credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='image_queue')

channel.basic_consume(queue='image_queue', on_message_callback=process_image_callback)

print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()