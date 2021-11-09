import pyspark


sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation")

log_of_songs = [
    "Despacito",
    "Nice for what",
    "No tears left to cry",
    "Despacito",
    "Havana",
    "In my feelings",
    "Nice for what",
    "despacito",
    "All the stars",
]

# parallelize the log_of_songs to use with Spark
distributed_song_log = sc.parallelize(log_of_songs)


def convert_song_to_lowercase(song):
    return song.lower()


distributed_song_log.map(convert_song_to_lowercase)

distributed_song_log.map(convert_song_to_lowercase).collect()

# The original data remains same
distributed_song_log.collect()


# Anonymous lamda function for same task
distributed_song_log.map(lambda song: song.lower()).collect()
