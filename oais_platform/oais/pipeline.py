"""
Example pipeline definition and resolver
"""


def get_next_steps(taskname):
    if taskname in [1, 2, 8]:
        return [3]
    elif taskname == 3:
        return [4]
    elif taskname == 4:
        return [5, 7]
    elif taskname == 7:
        return [5, 7]
    elif taskname == 5:
        return [5, 7]
