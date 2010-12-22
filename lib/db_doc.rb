require 'rexml/document'

class DbTasks
  class DbDoc
    # Define file tasks that will transform sql documentation from the
    # source directory to the target directory. The method will return
    # a list of the target files that will be created. The tool will
    # skip a directory if target == source as it can be difficult to
    # track and may be removed on clean
    def self.define_doc_tasks(source_directory, target_directory)
      target_files = []
      if File.expand_path(target_directory) != File.expand_path(target_directory)
        find_source_files(source_directory).collect do |src_file|
          # source file name with the source_directory path removed from its start
          src_file_name = src_file.to_s.gsub(/^#{source_directory.to_s}/, "")

          target_file = File.expand_path(target_directory + src_file_name.gsub(/\.sql$/, "_Documentation.sql"))

          file(target_file => [src_file]) do
            File.open(src_file) do |f|
              sql = f.readlines.join
              parsed_doc_models = []
              parse_sql_doc(sql, parsed_doc_models)
              generate_doc(target_file, parsed_doc_models)
            end
          end

          target_files << target_file
        end
      end

      target_files
    end

    private

    # Class holding documentation info for a given function or stored procedure
    class DocModel

      attr_accessor :full_object_name, :object_doc, :object_type, :param_docs

      def initialize
        @param_docs = {}
      end

      def add_param_doc(p_name, p_doc)
        @param_docs[p_name] = p_doc;
      end

      def object_name
        dot_index = full_object_name.index(".")
        if dot_index
          full_object_name[dot_index + 1, full_object_name.length]
        else
          full_object_name
        end
      end

      def schema_name
        dot_index = full_object_name.index(".")
        if dot_index
          full_object_name[0, dot_index]
        else
          "dbo"
        end
      end

      def to_s
        @full_object_name + ", " + @object_doc
      end
    end

    # Parses the given text file and fills the parse_models with DocModel
    # instances corresponding to all the documented functions and stored procedures
    # found in the file
    def self.parse_sql_doc(text, parsed_models)
      regexp = /(\/\*\*.*?\*\/)(\s*create\s+procedure\s+)([a-zA-Z_\.]+)/mi
      match = regexp.match(text)

      if !match
        regexp = /(\/\*\*.*?\*\/)(\s*create\s+function\s+)([a-zA-Z_\.]+)/mi
        match = regexp.match(text)
        if !match
          return
        end
      end

      comment_block = Regexp.last_match(1)
      create_stmt = Regexp.last_match(2)
      proc_name = Regexp.last_match(3)

      # strip the trailing */
      comment_block.gsub!(/\*\/\s*$/, "")

      # strip the leading /**
      comment_block.gsub!(/^\/\*\*/, "")

      # strip leading blank spaces for all lines
      comment_block.gsub!(/^\s*\*/, "")

      comment_xml = "<root>" + comment_block + "</root>"

      doc_model = DocModel.new
      doc_model.full_object_name = proc_name
      doc_model.object_type = /procedure/i.match(create_stmt) ? 'PROCEDURE' : 'FUNCTION'

      xml_doc = REXML::Document.new(comment_xml)
      root = xml_doc.root

      comment_desc = ""

      root.elements.each("//description") { |e| comment_desc = comment_desc + e.text.strip.gsub(/'/, "''") }

      doc_model.object_doc = comment_desc

      root.elements.each("//param") do |p|
        param_name = p.attributes["name"]
        doc_model.add_param_doc(param_name, p.text.strip.gsub(/'/, "''"))
      end

      parsed_models.push(doc_model)

      # the remained of the text file
      remainder = text[text.index(create_stmt + proc_name) + proc_name.length + create_stmt.length, text.length]

      # parse the remainder of the text file
      parse_sql_doc(remainder, parsed_models)

    end

    def self.generate_doc(target_file_name, doc_models)
      FileUtils.mkdir_p File.dirname(File.expand_path(target_file_name))

      # store all the doc models in the target_file_name
      # blank file if no documentation. Stops the need to keep re-running task
      File.open(target_file_name, 'wb') do |f|
        doc_models.each do |model|
          f.write <<SQL
EXEC sys.sp_addextendedproperty
  @name = N'MS_Description',
  @value = N'#{model.object_doc}',
  @level0type = N'SCHEMA', @level0name = '#{model.schema_name}',
  @level1type = N'#{model.object_type}', @level1name = '#{model.object_name}';
GO
SQL

          model.param_docs.each do |param, doc|
            f.write <<SQL
EXEC sys.sp_addextendedproperty
  @name = N'MS_Description',
  @value = N'#{doc}',
  @level0type = N'SCHEMA', @level0name = '#{model.schema_name}',
  @level1type = N'  #{model.object_type}  ', @level1name = '#{model.object_name}',
  @level2type = N'PARAMETER', @level2name = '@#{param}';
GO
SQL
          end
        end
      end
    end

    # do not include *_Documentation.sql files in the src files
    def self.find_source_files(source_directory)
      FileList["#{source_directory}/**/*.sql"].reject do |f|
        f_name = f.to_s
        /\_Documentation\.sql$/.match(f_name)
      end
    end
  end
end