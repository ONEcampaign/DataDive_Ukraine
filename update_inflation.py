"""Scripts to update the inflation data from WFP through a github action"""


from scripts.commodities_analysis import update_data


def last_updated():
    from csv import writer
    from scripts import config
    import datetime

    with open(config.paths.output + r"/updates.csv", "a+", newline="") as file:
        # Create a writer object from csv module
        csv_writer = writer(file)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.datetime.today()])


if __name__ == "__main__":

    update_data()

    # Save update time
    last_updated()
