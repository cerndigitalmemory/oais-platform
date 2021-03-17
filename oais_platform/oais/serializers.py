from django.contrib.auth.models import User, Group
from oais_platform.oais.models import Record
from rest_framework import serializers


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ['url', 'username', 'email', 'groups']


class GroupSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']


class RecordSerializer(serializers.HyperlinkedModelSerializer):
	class Meta:
		model = Record
		fields = ['url', 'id', 'source']