from collections import OrderedDict
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response


class EnvelopeLimitOffsetPagination(LimitOffsetPagination):
    default_limit = 25
    max_limit = 100

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ("data", data),
            ("pagination", OrderedDict([
                ("total", self.count),
                ("limit", self.limit),
                ("offset", self.offset),
            ]))
        ]))
