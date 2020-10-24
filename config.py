from configparser import ConfigParser

def Config(filename='database.ini', section='postgres'):
    parser = ConfigParser()
    parser.read(filename)

    db={}

    if parser.has_section(section):
        params = parser.items(section)

        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception('Section {0} is not found in the file {1}'.format(section, filename))

    return db

