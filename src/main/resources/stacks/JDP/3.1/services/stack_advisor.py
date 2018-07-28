#!/usr/bin/env ambari-python-wrap
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from resource_management.libraries.functions.get_bare_principal import get_bare_principal

class JDP30StackAdvisor(JDP21StackAdvisor):

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(JDP30StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "SUPERSET": self.recommendSupersetConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def recommendSupersetConfigurations(self, configurations, clusterData, services, hosts):
    # superset is in list of services to be installed
    if 'superset' in services['configurations']:
      # Recommendations for Superset
      superset_database_type = services['configurations']["superset"]["properties"]["SUPERSET_DATABASE_TYPE"]
      putSupersetProperty = self.putProperty(configurations, "superset", services)

      if superset_database_type == "mysql":
          putSupersetProperty("SUPERSET_DATABASE_PORT", "3306")
      elif superset_database_type == "postgresql":
          putSupersetProperty("SUPERSET_DATABASE_PORT", "5432")
