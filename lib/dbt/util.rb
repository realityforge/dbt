#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class Dbt
  class Util
    @@pre_1_zip_gem = false

    def self.use_pre_1_zip_gem!
      @@pre_1_zip_gem = true
    end

    def self.use_pre_1_zip_gem?
      @@pre_1_zip_gem.nil? ? (defined?(::Buildr) && ::Buildr::VERSION.to_s < '1.5.0') : !!@@pre_1_zip_gem
    end
  end
end
