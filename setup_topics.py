from kafka.admin import KafkaAdminClient,NewTopic
from config import KAFKA_BROKER,PARTITIONS

def create_topics():
    admin=KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id="topic-setup"
    )
    
    topic_list=[]
    for topic_name,partition_count in PARTITIONS.items():
        topic_list.append(
            NewTopic(
                name=topic_name,
                num_partitions=partition_count,
                replication_factor=1
            )
        )
    
    try:
        admin.create_topics(new_topics=topic_list,validate_only=False)
        print("Topics created successfully!")
        for t in topic_list:
            print(f"Topic: {t.name}, Partitions: {t.num_partitions}")
    except Exception as e:
        print(f"Error creating topics: {e}")
    
    finally:
        admin.close()
        
        
if __name__ == "__main__":
    create_topics()

    
    