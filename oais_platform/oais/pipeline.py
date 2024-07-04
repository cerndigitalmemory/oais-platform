"""
Example pipeline definition and resolver
"""


def get_next_steps(taskname):
    if taskname in [1, 2, 8]:
        return [3]
    elif taskname == 3:
        return [4]
    elif taskname in [4, 5, 7, 9, 10]:
        return [5, 7, 9]
