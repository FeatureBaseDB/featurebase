class REPL {
    constructor(input, output, button) {
        this.input = input
        this.output = output
        this.button = button
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
        var keywords = {
          // keyword: length of substring that comes after cursor
          "SetBit()": 1,
          "ClearBit()": 1,
          "SetBitmapAttrs()": 1,
          "Bitmap()": 1,
          "Union()": 1,
          "Intersect()": 1,
          "Difference()": 1,
          "Count()": 1,
          "Range()": 1,
          "TopN()": 1,
          "frame=": 0,
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

            // extract word fragment ending at cursor. a word fragment:
            // - starts with last nonalpha character before cursor (or beginning of string)
            // - ends at cursor
            var word_start = repl.input.selectionEnd-1
            while(word_start>0) {
              var c = repl.input.value.charCodeAt(word_start)
              if(!((c>64 && c<91) || (c>96 && c<123))) {
                word_start++
                break
              }
              word_start--
            }
            var input_word = repl.input.value.substring(word_start, repl.input.selectionEnd)

            // check for keyword match and insert
            // this just stops at the first match
            for(var keyword in keywords) {
              if(keyword.startsWith(input_word)){
                var cursor_pos = repl.input.selectionEnd
                var completion = keyword.substring(input_word.length)
                var before = repl.input.value.substring(0, cursor_pos)
                var after = repl.input.value.substring(cursor_pos)
                repl.input.value = before + completion + after
                var new_pos = cursor_pos + completion.length - keywords[keyword]
                repl.input.setSelectionRange(new_pos, new_pos)
                break
              }
            }
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
        var e = document.getElementById("index-dropdown");
        var indexname = e.options[e.selectedIndex].text;
        xhr.open('POST', '/index/' + indexname + '/query');
        xhr.setRequestHeader('Content-Type', 'application/text');

        var repl = this
        var start_time = new Date().getTime();
        xhr.send(query)
        xhr.onload = function() {
            var end_time = new Date().getTime()
            repl.result_number++
            repl.createSingleOutput({
              "input": query, 
              "output": xhr.responseText, 
              "indexname": indexname,
              "querytime_ms": end_time - start_time,
            })
        }

    }

    createSingleOutput(res) {
      var node = document.createElement("div");
      node.classList.add('output');
      var output_string = res['output']
      var output_json = JSON.parse(output_string)
      var result_class = "result-output"
      var getting_started_errors = [
        'index not found',
        'frame not found',
      ]

      if("error" in output_json) {
        result_class = "result-error"
        if(getting_started_errors.indexOf(output_json['error']) >= 0) {  
          output_string += `<br />
          <br />
          Just getting started? Try this:<br />
          $ curl -XPOST "http://127.0.0.1:10101/index/test" -d '{"options": {"columnLabel": "col"}}' # create index "test"<br />
          $ curl -XPOST "http://127.0.0.1:10101/index/test/frame/foo" -d '{"options": {"rowLabel": "row"}}' # create frame "foo"<br />
          # Select "test" in the index dropdown above<br />
          SetBit(row=0, col=0, frame=foo) # Use PQL to set a bit
          `
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
      this.output.insertBefore(node, this.output.firstChild)
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
      version = JSON.parse(xhr.responseText)['version']
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
  time_node = document.getElementById('status-time')
  xhr.onload = function() {
    var status = JSON.parse(xhr.responseText)
    render_status(status)
    time_node.innerHTML = new Date().today() + " " + new Date().timeNow()
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
  tbody.appendChild(caption)

  var header = document.createElement('tr')
  markup = `<th>Host</th>
  <th>State</th>`
  header.innerHTML = markup
  table.appendChild(header)
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
    caption.innerHTML = indexes[n]["Name"] + " (Column Label: " + indexes[n]["Meta"]["ColumnLabel"] + ")"
    table.appendChild(caption)

    var header = document.createElement('tr')
    markup = `<th>Name</th>
    <th>Row Label</th>
    <th>Cache Type</th>
    <th>Cache Size</th>`
    header.innerHTML = markup
    tbody.appendChild(header)

    var frames = indexes[n]["Frames"]
    if(frames) {
      for(var m=0; m<frames.length; m++) {
        var row = document.createElement("tr")
        row.innerHTML = `<td>${frames[m]["Name"]}</td>
        <td>${frames[m]["Meta"]["RowLabel"]}</td>
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

var input = document.getElementById('query')
var output = document.getElementById('outputs')
var button = document.getElementById('query-btn')

repl = new REPL(input, output, button)
repl.populate_index_dropdown()
repl.bind_events()

input.focus()

check_anchor_uri()
