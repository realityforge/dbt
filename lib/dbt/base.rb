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

class Dbt #nodoc

  # Base class used for named elements configurable via options
  class BaseElement < Reality::BaseElement
    attr_reader :key

    def initialize(key, options, &block)
      @key = key
      super(options)
    end
  end

  # Base Class used for sub-elements of database
  class DatabaseElement < Dbt::BaseElement

    def initialize(database, key, options, &block)
      @database = database
      super(key, options, &block)
    end

    attr_reader :database
  end
end
