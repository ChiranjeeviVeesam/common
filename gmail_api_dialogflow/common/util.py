from configparser import ConfigParser


def config_info(
    configuration_path = 'configuration.cfg'
):
    parser = ConfigParser()
    parser.read(configuration_path)
    return parser 