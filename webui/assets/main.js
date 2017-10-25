class REPL {
    constructor(input, output, button, completer) {
        this.input = input
        this.output = output
        this.button = button
        this.completer = completer
        this.history = []
        this.history_index = 0
        this.history_buffer = ''
        this.result_number = 0
    }
    bind_events() {
        var repl = this
        var keys = {
          TAB: 9,
          ENTER: 13,
          UP_ARROW: 38,
          DOWN_ARROW: 40
        }

        this.input.addEventListener("keydown", function(e) {
          if (e.keyCode == keys.UP_ARROW) {
            e.preventDefault()
            if (repl.input.value.substring(0, repl.input.selectionStart).indexOf('\n') == '-1') {
                if (repl.history_index == 0) {
                    return
                } else {
                    if (repl.history_index == repl.history.length) {
                        repl.history_buffer = repl.input.value
                    }
                    repl.history_index--
                    repl.input.value = repl.history[repl.history_index]
                    repl.input.setSelectionRange(repl.input.value.length, repl.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.DOWN_ARROW) {
            e.preventDefault()
            if (repl.input.value.substring(repl.input.selectionEnd, repl.input.length).indexOf('\n') == '-1') {
                if (repl.history_index == repl.history.length) {
                    return
                } else {
                    repl.history_index++
                    if (repl.history_index == repl.history.length) {
                        repl.input.value = repl.history_buffer
                    } else {
                        repl.input.value = repl.history[repl.history_index]
                    }
                    repl.input.setSelectionRange(repl.input.value.length, repl.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.ENTER && !e.shiftKey) {
            e.preventDefault()
            repl.submit();
          }
          if (e.keyCode == keys.TAB) {
            e.preventDefault()
            repl.completer.complete()
          }
        })
        repl.button.onclick = function() {
            repl.submit();
        };
    }

    submit() {
        this.history_buffer = ''
        this.history_index = this.history.length
        this.history[this.history_index] = this.input.value
        this.history_index++
        this.process_query(this.input.value)
        this.input.value = ""
    }

    process_query(query) {
        var xhr = new XMLHttpRequest();
        var url, data, request, command_name;
        var e = document.getElementById("index-dropdown");
        var indexname = e.options[e.selectedIndex].text;
        var repl = this;
        if (query.startsWith(":")) {
            var parsed_query = parse_query(query, indexname);
            if (Object.keys(parsed_query).length === 0) {
                repl.create_single_output({
                    "input": query,
                    "output": "invalid query",
                    "status": 400,
                    "indexname": indexname,
                });
                return;
            } else {
                // set selectedIndex from dropdown list
                if (parsed_query.command === "use") {
                    for (var i = 0; i < e.options.length; i++) {
                        if (e.options[i].text === parsed_query.command_name) {
                            e.selectedIndex = i;
                            break;
                        }
                    }
                    return;
                }
                url = parsed_query.url;
                data = parsed_query.data;
                request = parsed_query.request;
                command_name = parsed_query.command_name;
            }
        }
        else {
            url = '/index/' + indexname + '/query'
            request = "POST"
            data = query
        }
        xhr.open(request, url);
        xhr.setRequestHeader('Content-Type', 'application/text');

        var start_time = new Date().getTime();
        xhr.onload = function () {
            var end_time = new Date().getTime();
            repl.result_number++
            repl.create_single_output({
                "input": query,
                "output": xhr.responseText,
                "status": xhr.status,
                "indexname": indexname,
                "querytime_ms": end_time - start_time,
            });
        };

        xhr.send(data);
        // Remove index from dropdown with delete index command
        if (request === 'DELETE' && url === '/index/' + command_name){
            for (var i = 0; i < e.options.length; i++) {
                if (e.options[i].text === command_name) {
                    e.remove(i);
                    break;
                }
            }
        }
    }

    create_single_output(res) {
        var node = document.createElement("div");
        node.classList.add('output');
        var output_string = res['output']
        var result_class = "result-output"
        var getting_started_errors = [
            'index not found',
            'frame not found',
        ]
        var output_json;
        if (isJSON(output_string)) {
            output_json = JSON.parse(output_string)
        }
        // handle output formatting
        if (res["status"] != 200) {
            result_class = "result-error";
            if (output_json) {
                if ("error" in output_json) {
                    if (getting_started_errors.indexOf(output_json['error']) >= 0) {
                        output_string += `<br />
          <br />
          Just getting started? Try this:<br />
          :create index test<br />
          :use test<br />
          :create frame foo<br />
          SetBit(rowID=0, columnID=0, frame=foo) # Use PQL to set a bit
          `
                    }
                }
            }
        }


      var markup =`
        <div  class="panes">
          <div class="pane active">
            <div class="result-io">
              <div class="result-io-header">
                <h5>Input</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>Source: ${res.indexname}</em>
              </div>
              <div class="result-input">
                ${res.input}
              </div>
            </div>
            <div class="result-io">
              <div class="result-io-header">
                <h5>output</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>${res.querytime_ms} ms</em>
              </div>
              <div class="${result_class}">
                ${output_string}
                </div>
              <a href="#" class="expand"><h5>Expand</h5></a>
              
            </div>
          </div>
          <div class="pane">
            <div class="raw">
    
              <a href="${res.url}">Export raw .csv</a>
            </div>
          </div>
        </div>
      `
        node.innerHTML = markup;
        this.output.insertBefore(node, this.output.firstChild);

        // Expand when overflow
        var element = this.output.firstChild.getElementsByClassName(result_class)[0];
        var expand = this.output.firstChild.getElementsByClassName("expand")[0];
        if (element.clientHeight < element.scrollHeight) {
            expand.style.display = 'block';
        } else {
            expand.style.display = 'none';
        }
        expand.onclick = function () {
            element.style.height = element.scrollHeight + "px";
            expand.style.display = 'none';
            return false;
        };
    }

    populate_index_dropdown() {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', '/schema')
      var select = document.getElementById('index-dropdown')

      xhr.onload = function() {
        var schema = JSON.parse(xhr.responseText)
        for(var i=0; i<schema['indexes'].length; i++) {
          var opt = document.createElement('option')
          opt.value = i+1
          opt.innerHTML = schema['indexes'][i]['name']
          select.appendChild(opt)
        }
        // set the active option to one of the populated options, instead of invalid "Index"
        if(i > 0) {
          select.value = 1;
        }
      }
      xhr.send(null)
    }

}

function populate_version() {
  var xhr = new XMLHttpRequest();
    xhr.open('GET', '/version')
    var node = document.getElementById('server-version')

    xhr.onload = function() {
      var version = JSON.parse(xhr.responseText)['version']
      var version_major_minor = /(v\d+\.\d+)/.exec(version)[0]
      var doc_link = document.getElementById('nav-documentation')
      doc_link.onclick = function() {
        window.open('https://www.pilosa.com/docs/' + version_major_minor + '/introduction/')
      }
      node.innerHTML = version
    }
    xhr.send(null)
}

function handle_nav_click(e) {
  // e.id = "nav-xxx"
  name = e.id.substring(4)
  set_active_pane_by_name(name)
  window.location.hash = name
}

function set_active_pane_by_name(name) {
  // toggle the nav buttons
  document.getElementsByClassName("nav-active")[0].classList.remove("nav-active")
  document.getElementById("nav-" + name).classList.add("nav-active")

  // toggle the main interface content divs
  document.getElementsByClassName("interface-active")[0].classList.remove("interface-active")
  document.getElementById('interface-' + name).classList.add("interface-active")

  // hack hack
  switch(name) {
    case "cluster":
      update_cluster_status()
      break
    case "documentation":
      open_external_docs()
      break
  }
}


function update_cluster_status() {
  var xhr = new XMLHttpRequest();
  xhr.open('GET', '/status')
  status_node = document.getElementById('status')
  xhr.onload = function() {
    var status = JSON.parse(xhr.responseText)
    render_status(status)
  }
  xhr.send(null)
}

function render_status(status) {
  // render node table
  var nodes_div = document.getElementById("status-nodes")
  while (nodes_div.firstChild) {
    nodes_div.removeChild(nodes_div.firstChild);
  }

  var nodes = status["status"]["Nodes"]
  table = document.createElement("table")
  tbody = document.createElement("tbody")
  table.appendChild(tbody)
  var caption = document.createElement("caption")
  caption.innerHTML = "(" + nodes.length + ")"
  table.appendChild(caption)

  var header = document.createElement('tr')
  markup = `<th>Host</th>
  <th>State</th>`
  header.innerHTML = markup
  tbody.appendChild(header)
  for(var n=0; n<nodes.length; n++) {
    var row = document.createElement("tr")
    markup = `<td>${nodes[n]["Host"]}</td>
    <td>${nodes[n]["State"]}</td>`
    row.innerHTML = markup
    tbody.appendChild(row)
  }
  nodes_div.appendChild(table)

  // render index tables
  var indexes_div = document.getElementById("status-indexes")
  while (indexes_div.firstChild) {
    indexes_div.removeChild(indexes_div.firstChild);
  }

  var indexes = nodes[0]["Indexes"] // TODO currently comes from only node 0
  for(var n=0; n<indexes.length; n++) {
    table = document.createElement("table")
    tbody = document.createElement("tbody")
    table.appendChild(tbody)
    var caption = document.createElement("caption")
    caption.innerHTML = indexes[n]["Name"]
    table.appendChild(caption)

    var header = document.createElement('tr')
    markup = `<th>Name</th>
    <th>Cache Type</th>
    <th>Cache Size</th>`
    header.innerHTML = markup
    tbody.appendChild(header)

    var frames = indexes[n]["Frames"]
    if(frames) {
      for(var m=0; m<frames.length; m++) {
        var row = document.createElement("tr")
        row.innerHTML = `<td>${frames[m]["Name"]}</td>
        <td>${frames[m]["Meta"]["CacheType"]}</td>
        <td>${frames[m]["Meta"]["CacheSize"]}</td>`
        tbody.appendChild(row)
      }
    }
    indexes_div.appendChild(table)
  }

  // render slice tables
  // TODO enable when Slices element is present in status response
  /*
  var slices_div = document.getElementById("status-slices")
  data = ""
  for(var n=0; n<nodes.length; n++) {
    var indexes = nodes[n]["Indexes"]
    for(var m=0; m<indexes.length; m++) {
      data += nodes[n]["Host"] + "/" + indexes[m]["Name"] + ": " + indexes[m]["Slices"] + "<br />"
    }
  }
  slices_div.innerHTML = data
  */

}

function open_external_docs() {
  window.open("https://www.pilosa.com/docs");
}

function check_anchor_uri() {
  var pane_names = {"console": 0, "cluster": 0, "documentation": 0}
  var anchor = window.location.hash.substr(1);
  if(anchor in pane_names) {
    set_active_pane_by_name(anchor)
  }
}

Date.prototype.today = function () {
    return this.getFullYear() +"/"+ (((this.getMonth()+1) < 10)?"0":"") + (this.getMonth()+1) +"/"+ ((this.getDate() < 10)?"0":"") + this.getDate();
}

Date.prototype.timeNow = function () {
     return ((this.getHours() < 10)?"0":"") + this.getHours() +":"+ ((this.getMinutes() < 10)?"0":"") + this.getMinutes() +":"+ ((this.getSeconds() < 10)?"0":"") + this.getSeconds();
}

populate_version()


class Autocompleter {
  constructor(input, output) {
    this.input = input
    this.output = output
    this.keyword_map = this.static_keywords
    this.init_dynamic_keywords()
  }

  get static_keywords() {
    return {
      // keyword: length of substring that comes after cursor
      "SetBit()": 1,
      "ClearBit()": 1,
      "SetRowAttrs()": 1,
      "SetColumnAttrs()": 1,
      "Bitmap()": 1,
      "Union()": 1,
      "Intersect()": 1,
      "Difference()": 1,
      "Count()": 1,
      "Range()": 1,
      "TopN()": 1,
      "frame=": 0,
    }
  }

  complete() {
    var completer = this
    // extract word fragment ending at cursor. a word fragment:
    // - starts with last nonalpha character before cursor (or beginning of string)
    // - ends at cursor
    var word_start = completer.input.selectionEnd-1
    while(word_start>0) {
      var c = completer.input.value.charCodeAt(word_start)
      if(!((c>64 && c<91) || (c>96 && c<123))) {
        word_start++
        break
      }
      word_start--
    }
    var input_word = completer.input.value.substring(word_start, completer.input.selectionEnd)

    // check for keyword match and insert if exactly one match
    var matches = []
    for(var keyword in this.keyword_map) {
      if(keyword.startsWith(input_word)){
        matches.push(keyword)
      }
    }
    if(matches.length > 1) {
      // completer.output.innerHTML = whatever
    }

    if(matches.length == 1) {
      // completer.output.innerHTML = ""
      var cursor_pos = completer.input.selectionEnd
      var completion = matches[0].substring(input_word.length)
      var before = completer.input.value.substring(0, cursor_pos)
      var after = completer.input.value.substring(cursor_pos)
      completer.input.value = before + completion + after
      var new_pos = cursor_pos + completion.length - this.keyword_map[matches[0]]
      completer.input.setSelectionRange(new_pos, new_pos)
    }
  }

  init_dynamic_keywords() {
    // hit /schema, parse indexes, frames, rowlabels, columnlabels, add to list
  }

  add_keyword() {
    // call when index or frame created in webui
  }

  remove_keyword() {
    // call when index or frame deleted in webui
    // issue: if e.g. multiple indexes have same frame, removing one removes all.
    // solution: maintain count. requires more elaborate representation of keywords.
  }
}

var input = document.getElementById('query')
var output = document.getElementById('outputs')
var button = document.getElementById('query-btn')
var autocomplete_output = document.getElementById('autocomplete-container')

autocompleter = new Autocompleter(input, autocomplete_output)
repl = new REPL(input, output, button, autocompleter)
repl.populate_index_dropdown()
repl.bind_events()

input.focus()

check_anchor_uri()

function isJSON(str) {
    try {
        JSON.parse(str)
    } catch (e) {
        return false
    }
    return true
}

function parse_query(query, indexname) {
    var keys = query.replace(/\s+/g, " ").split(" ");
    var command = keys[0];
    var command_type = keys[1];
    var command_name = keys[2];
    var option_str = keys.slice(3, keys.length)
    var options = parse_options(option_str);
    if (command !== ":use") {
         if (!command_name){
            return {}
        }
    }

    var parsed_query = {};
    parsed_query["command"] = command.substr(1, command.length);
    parsed_query["command_name"] = command_name;
    switch (command) {
        case ":create":
            parsed_query["request"] = "POST";
            if(Object.keys(options).length === 0) {
                parsed_query["data"] = "";
            } else {
                var opts = {"options":{}};
                for (var o in options) {
                    opts.options[o] = options[o]
                }
                parsed_query["data"] = JSON.stringify(opts);
            }
            switch (command_type){
                case "index":
                    parsed_query["url"] =  '/index/' + command_name;
                    break;
                case "frame":
                    parsed_query["url"] =  '/index/' + indexname + '/frame/' + command_name;
                    break
            }
            break;
        case ":delete":
            parsed_query["request"] = "DELETE";
            switch (command_type){
                case "index":
                    parsed_query["url"] =  '/index/' + command_name;
                    parsed_query["data"] = "";
                    break;
                case "frame":
                    parsed_query["url"] =  '/index/' + indexname + '/frame/' + command_name;
                    parsed_query["data"] = "";
                    break;
            }
            break;
        case ":use":
            parsed_query["command_name"] = keys[1];
            break;
        default:
            return {}
    }
    return parsed_query;
}

function parse_options(option_str) {
    var int_keys = ["cacheSize"];
    var bool_keys = ["inverseEnabled"];
    var options = {};
        for (var i = 0; i < option_str.length; i++) {
            var parts = option_str[i].split('=');
            if (int_keys.indexOf(parts[0]) !== -1 ){
                options[parts[0]] = Number(parts[1])
            } else if (bool_keys.indexOf(parts[0]) !== -1){
                 options[parts[0]] = (parts[1] == "true")
            } else {
                options[parts[0]] = parts[1]
            }
        }
        return options;
}