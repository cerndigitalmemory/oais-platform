"""
Example pipeline definition and resolver

Step enums are defined in models.py Steps class
To avoid circular imports:
1 - SIP upload
2 - Harvest
3 - Validation
4 - Checksum
5 - Archive
6 - Edit manifest
7 - Push to registry
8 - Announce
9 - Push to CTA
10 - Extract title
11 - Notify source
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
    11: [5, 7, 9],
}


def get_next_steps(step_name):
    return next_steps_constraints.get(step_name, [])


def get_next_steps_constraints():
    return next_steps_constraints
