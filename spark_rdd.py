def count_elements_in_dataset(dataset):
    """
    Given a dataset loaded on Spark, return the
    number of elements.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: number of elements in the RDD
    """

    return dataset.count()


def get_first_element(dataset):
    """
    Given a dataset loaded on Spark, return the
    first element
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: the first element of the RDD
    """

    return dataset.first()


def get_all_attributes(dataset):
    """
    Each element is a dictionary of attributes and their values for a post.
    Can you find the set of all attributes used throughout the RDD?
    The function dictionary.keys() gives you the list of attributes of a dictionary.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: all unique attributes collected in a list
    """

    distinct_keys = dataset.flatMap(lambda line: line.keys()).distinct()

    return distinct_keys.collect()


def get_elements_w_same_attributes(dataset):
    """
    We see that there are more attributes than just the one used in the first element.
    This function should return all elements that have the same attributes
    as the first element.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD containing only elements with same attributes as the
    first element
    """

    first = dataset.first()
    same_attrib_as_first = dataset.filter(lambda line: line.keys() == first.keys())

    return same_attrib_as_first


def get_min_max_timestamps(dataset):
    """
    Find the minimum and maximum timestamp in the dataset
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: min and max timestamp in a tuple object
    :rtype: tuple
    """

    dtimes = dataset.map(lambda line: line['created_at_i'])
    min_time = dtimes.min()
    max_time = dtimes.max()

    return extract_time(min_time), extract_time(max_time)


def get_number_of_posts_per_bucket(dataset, min_time, max_time):
    """
    Using the `get_bucket` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements that fall within each bucket.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :param min_time: Minimum time to consider for buckets (datetime format)
    :param max_time: Maximum time to consider for buckets (datetime format)
    :return: an RDD with number of elements per bucket
    """

    dtimes = dataset.map(lambda line: line['created_at_i'])
    min_time = dtimes.min()
    max_time = dtimes.max()

    buckets_rdd = (
        dataset.map(lambda line: (get_bucket(line, min_time, max_time), 1))
        .reduceByKey(lambda c1, c2: c1 + c2)
    )

    return buckets_rdd


def get_number_of_posts_per_hour(dataset):
    """
    Using the `get_hour` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with number of elements per hour
    """

    hours_buckets_rdd = (
        dataset.map(lambda line: (get_hour(line), 1))
        .reduceByKey(lambda c1, c2: c1 + c2)
    )

    return hours_buckets_rdd


def get_score_per_hour(dataset):
    """
    The number of points scored by a post is under the attribute `points`.
    Use it to compute the average score received by submissions for each hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with average score per hour
    """

    scores_per_hour_rdd = (dataset
                           .map(lambda line: (get_hour(line), (line['points'], 1)))
                           .reduceByKey(lambda acc, val: (acc[0] + val[0], acc[1] + val[1]))
                           .mapValues(lambda v: v[0] / v[1])
                           )

    return scores_per_hour_rdd


def get_proportion_of_scores(dataset):
    """
    It may be more useful to look at sucessful posts that get over 200 points.
    Find the proportion of posts that get above 200 points per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of scores over 200 per hour
    """

    prop_per_hour_rdd = (
        dataset.map(lambda line: (get_hour(line), (1, popular(line['points']))))
        .reduceByKey(lambda acc, vals: (acc[0] + vals[0], acc[1] + vals[1]))
        .mapValues(lambda v: v[1] / v[0])
    )

    return prop_per_hour_rdd


def get_proportion_of_success(dataset):
    """
    Using the `get_words` function defined in the notebook to count the
    number of words in the title of each post, look at the proportion
    of successful posts for each title length

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of successful post per title length
    """

    prop_per_title_length_rdd = (
        dataset.map(lambda line: (count_words(line), (successful(line), 1)))
        .reduceByKey(lambda acc, vals: (acc[0] + vals[0], acc[1] + vals[1]))
        .mapValues(lambda v: v[0] / v[1])
    )

    return prop_per_title_length_rdd


def get_title_length_distribution(dataset):
    """
    Count for each title length the number of submissions with that length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the number of submissions per title length
    """

    submissions_per_length_rdd = (
        dataset.map(lambda line: (get_words(line), 1))
        .reduceByKey(lambda acc, val: acc + val)
    )

    return submissions_per_length_rdd


def get_bucket(rec, min_timestamp, max_timestamp):
    interval = (max_timestamp - min_timestamp + 1) / 200.0
    return int((rec['created_at_i'] - min_timestamp)/interval)


def get_hour(rec):
    from datetime import datetime as dt

    t = dt.utcfromtimestamp(rec['created_at_i'])
    return t.hour


def popular(x):
    if x > 200:
        return 1
    else:
        return 0


def extract_time(timestamp):
    from datetime import datetime as dt

    return dt.utcfromtimestamp(timestamp)


def get_words(line):
    import re
    if 'title' in line.keys():
        return len(re.compile('\w+').findall(line['title']))
    else:
        return 0


def count_words(line):
    if 'title' in line.keys():
        return get_words(line)
    else:
        return 0


def successful(line):
    if line['points'] > 200:
        return 1
    else:
        return 0
