import logging

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

class Client_Menu:

    def __init__(self):
        pass

    @staticmethod
    def display_menu():
        # logger.info("- Press 1 to CreateGroup") createGroup 1 2 3 - done
        # logger.info("- Press 2 to Add Member")  add 2_1 5 - done
        # logger.info("- Press 3 to Kick Member") kick 2_1 5 - done
        # logger.info("- Press 4 to Write Message")
        # logger.info("- Press 5 to Print Group") printGroup 2_1
        # logger.info("- Press 6 to Fail Link")
        # logger.info("- Press 7 to Fix Link")
        # logger.info("- Press 8 to Fail Process") - done
        logger.info(" -- Choose from the following commands -- ")
        logger.info(" - createGroup <client_ids>")
        logger.info(" - add <group id> <client id>")
        logger.info(" - kick <group id> <client id>")
        logger.info(" - writeMessage <group id> <message>")
        logger.info(" - printGroup <group id>")
        logger.info(" - failLink <src> <dest>")
        logger.info(" - fixLink <src> <dest>")
        logger.info(" - failProcess")
        # TODO : delete this later
        logger.info("- Press 9 to display logs")
