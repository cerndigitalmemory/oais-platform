"""
Example pipeline definition and resolver
"""

next_steps_constraints = {
    1: [3],
    2: [3],
    3: [4],
    4: [5, 7],
    5: [5, 7, 9],
    7: [5, 7],
    8: [3],
    9: [5, 7, 9],
    10: [5, 7],
}


def get_next_steps(step_name):
    return next_steps_constraints.get(step_name, [])


def get_next_steps_constraints():
    return next_steps_constraints
