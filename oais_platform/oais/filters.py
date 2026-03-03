from django.db.models import Exists, OuterRef, Q

from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.models import Step
from oais_platform.settings import STEP_FILTER_CONDITION_LIMIT


def count_conditions(group):
    if "and" in group:
        # Count all conditions within the AND group
        return sum(count_conditions(item) for item in group["and"])
    elif "or" in group:
        # Count all conditions within the OR group
        return sum(count_conditions(item) for item in group["or"])
    else:
        return 1


def validate_step_group(group):
    total = count_conditions(group)
    if total > STEP_FILTER_CONDITION_LIMIT:
        raise BadRequest(
            f"Maximum {STEP_FILTER_CONDITION_LIMIT} boolean combine groups allowed"
        )


filters_map = {
    "name": ["step_type__name"],
    "status": ["status"],
}


def build_step_condition(condition):
    """
    Builds a Q object for a single step condition.
    Returns a Q object ready to apply to Archive queryset.
    """

    exclude = condition.pop("exclude", False)
    is_last_step = condition.pop("last_step", False)

    if is_last_step:
        q = Q()

        for key, value in condition.items():
            if key not in filters_map:
                raise KeyError(f"Invalid filter key: {key}")
            for query_key in filters_map[key]:
                q &= Q(**{f"last_step__{query_key}": value})

        return ~q if exclude else q

    subquery = Step.objects.filter(archive=OuterRef("pk"))

    for key, value in condition.items():
        if key not in filters_map:
            raise KeyError(f"Invalid filter key: {key}")
        for query_key in filters_map[key]:
            subquery = subquery.filter(**{query_key: value})

    exists_q = Q(Exists(subquery))

    return ~exists_q if exclude else exists_q


def build_step_group(group):
    """
    Recursively builds AND/OR groups.
    """

    if "and" in group:
        q = Q()
        for item in group["and"]:
            q &= build_step_group(item)
        return q

    if "or" in group:
        q = Q()
        first = True
        for item in group["or"]:
            if first:
                q = build_step_group(item)
                first = False
            else:
                q |= build_step_group(item)
        return q

    return build_step_condition(group)
