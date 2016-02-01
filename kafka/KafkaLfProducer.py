import sys
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
from ConfigParser import SafeConfigParser
from MeterReader import MeterLfReader

class KafkaLfProducer(object):
    def __init__(self, addr, conf_file, start_house_id, end_house_id, house_status):
        self.parser = SafeConfigParser()
        self.parser.read(conf_file)
        install_dir = self.parser.get('smw_tool', 'INSTALL_DIR')
        zipdb_file = self.parser.get('smw_tool', 'ZIP_DB_FILE') 

        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client, async=True)
        self.meterReader = MeterLfReader(start_house_id,
                                         end_house_id,
                                         house_status,
                                         install_dir + "/data/low_freq/", 
                                         install_dir + "/" + zipdb_file)

    def produce_msgs(self, source_symbol):
        msg_cnt = 0

        while not self.meterReader.houseSentDone():
            (isLf, msg) = self.meterReader.getRecord()

            if msg_cnt % 500000 == 0:
                print "Sent " + str(msg_cnt) + " messages to Kafka"

            if isLf:
                self.producer.send_messages('smw_batch_lf', source_symbol, msg)
            else:
                self.producer.send_messages('smw_batch_hf', source_symbol, msg)

            msg_cnt += 1

        print "Sent Total " + str(msg_cnt) + " messages to Kafka"
        self.meterReader.writeHouseStatus()

if __name__ == "__main__":
    args = sys.argv
    conf_file = str(args[1])
    ip_addr = str(args[2])
    partition_key = str(args[3])
    start_house_id = int(args[4])
    end_house_id = int(args[5])
    house_status = str(args[6])
    prod = KafkaLfProducer(ip_addr, conf_file, start_house_id, end_house_id, house_status)
    prod.produce_msgs(partition_key) 
