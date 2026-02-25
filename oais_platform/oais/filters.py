from django.db.models import Exists, OuterRef, Q

from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.models import Step
from oais_platform.settings import STEP_FILTER_COMBINATION_LIMIT


def count_combines(group):
    """
    Recursively count total AND/OR groups in tree.
    """
    count = 0

    if "and" in group:
        count += 1
        for item in group["and"]:
            count += count_combines(item)

    elif "or" in group:
        count += 1
        for item in group["or"]:
            count += count_combines(item)

    return count


def validate_step_group(group):
    total = count_combines(group)
    if total > STEP_FILTER_COMBINATION_LIMIT:
        raise BadRequest(
            f"Maximum {STEP_FILTER_COMBINATION_LIMIT} boolean combine groups allowed"
        )


def build_step_condition(condition):
    """
    Builds a Q object for a single step condition.
    Returns a Q object ready to apply to Archive queryset.
    """

    exclude = condition.get("exclude", False)
    is_last_step = condition.get("last_step", False)

    if is_last_step:
        q = Q()

        if "name" in condition:
            q &= Q(last_step__step_type__name=condition["name"])

        if "status" in condition:
            q &= Q(last_step__status=condition["status"])

        return ~q if exclude else q

    subquery = Step.objects.filter(archive=OuterRef("pk"))

    if "name" in condition:
        subquery = subquery.filter(step_type__name=condition["name"])

    if "status" in condition:
        subquery = subquery.filter(status=condition["status"])

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
