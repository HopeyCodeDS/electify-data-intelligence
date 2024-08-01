from configparser import ConfigParser

def load_config(filename='databaseDWH.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def load_config2(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def load_config_test(filename='database_test.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def load_config_gcloud(filename='databaseDWHgcloud.ini', section='gcloud-postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to gcloud-postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def load_config_prodDB(filename='productionDB.ini', section='productionDBcloud'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to production DB in gcloud
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

if __name__ == '__main__':
    try:
        config = load_config()
        config2 = load_config2()
        config3 = load_config_test()
        config4 = load_config_gcloud()
        config5 = load_config_prodDB()
        print("Config loaded successfully:")
        print(config)
        print("Config2 loaded successfully:")
        print(config2)
        print("Config_test loaded successfully:")
        print(config3)
        print("Config_gcloud loaded successfully:")
        print(config4)
        print("Config_production DB loaded successfully:")
        print(config5)
    except Exception as e:
        print("Error loading config:", e)
