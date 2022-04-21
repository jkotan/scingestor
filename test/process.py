import time

# ingestor service object
ingestor = None

# current beamtime
beamtime = None

# ingest after each scan
ingest_after_scan = True

# ingest at the end of beamtime
ingest_end_beamtime = True


class Ingestor:
    """ ingestor service """

    def __init__(self, after_scan=True, end_beamtime=True):
        """ ingestor constructor """

        self.ingested = []
        self.waiting = []
        self.last_time = 0
        self.active = after_scan
        self.reingestion = end_beamtime

    def append_scan(self, scan_name):
        """ append scan to ingest """

        self.waiting.append(scan_name)
        self.last_time = time.time()

    def run(self):
        """ ingestor thread/process"""

        while self.active:
            if self.waiting and self.last_time \
               and time.time() > self.last_time + 60:
                scan_name = self.waiting.pop()
                self.ingest(scan_name)
                self.ingested.append(scan_name)

    def stop(self):
        """ stop ingestor thread/process """

        self.active = False

    def ingest(self, scan_name):
        """ ingest a single dataset and the corresponding datablock """

        pid = ingest_dataset(scan_name)
        ingest_datablock(scan_name, pid)

    def reingest(self, scan_name):
        """ reingest all single dataset
            and the corresponding datablock if needed
        """

        if self.reingestion:
            for sc in (set(self.waiting) | set(self.ingested)):
                if metadata_changed(scan_name):
                    pid = ingest_dataset(scan_name)
                else:
                    pid = get_dataset_pid(scan_name)
                if datablock_changed(scan_name):
                    ingest_datablock(scan_name, pid)


def start_ingestor_service():
    """ start ingeston service  """

    global ingestor
    ingestor = Ingestor(ingest_after_scan, ingest_end_beamtime)
    ingestor.start()


def start_beamtime():
    """ start beatime script """
    global beamtime, ingestor

    beamtime = generate_beamtime()
    create_groups()
    ingest_proposal()

    ingestor = start_ingestor_service()


def scan(scan_file, scan_id):
    """ single scan """

    perform_scan(scan_file, scan_id)
    ingestor.append_scan("%s_%s" % (scan_file, scan_id))


def perform_scan(scan_file, scan_id):
    """ perform sardana scan """


def stop_beamtime():
    """ stop beamtime script   """
    global beamtime

    ingestor.stop()
    ingestor.join()
    ingestor.reingest()
    beamtime = None


def main():
    """  beamtime  """

    start_beamtime()

    scan_file = ""
    scan_max_id = 12345

    for scan_id in range(scan_max_id):
        scan(scan_file, scan_id)

    stop_beamtime()


def create_groups():
    """ create scicat groups"""


def get_dataset_pid(scan_name):
    """ get dataset pid"""


def generate_beamtime():
    """ generate beamtime"""


def datablock_changed():
    """ do datafiles changed"""


def metadata_changed():
    """ do metadata changed"""


def ingest_proposal():
    """ ingest proposal"""


def ingest_dataset(scan_name):
    """ ingest dataset"""


def ingest_datablock(scan_name):
    """ ingest datablock"""
