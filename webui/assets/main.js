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
        const repl = this
        const keys = {
          TAB: 9,
          ENTER: 13,
          UP_ARROW: 38,
          DOWN_ARROW: 40
        }
        const keywords = {
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

        this.input.addEventListener("keydown", (e) => {
          if (e.keyCode == keys.UP_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(0, this.input.selectionStart).indexOf('\n') == '-1') {
                if (this.history_index == 0) {
                    return
                } else {
                    if (this.history_index == this.history.length) {
                        this.history_buffer = this.input.value
                    }
                    this.history_index--
                    this.input.value = this.history[this.history_index]
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.DOWN_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(this.input.selectionEnd, this.input.length).indexOf('\n') == '-1') {
                if (this.history_index == this.history.length) {
                    return
                } else {
                    this.history_index++
                    if (this.history_index == this.history.length) {
                        this.input.value = this.history_buffer
                    } else {
                        this.input.value = this.history[this.history_index]
                    }
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.ENTER && !e.shiftKey) {
            e.preventDefault()
            this.submit();
          }
          if (e.keyCode == keys.TAB) {
            e.preventDefault()

            // extract word ending at cursor
            var word_start = this.input.selectionEnd-1
            while(word_start>0) {
              var c = this.input.value.charCodeAt(word_start)
              if(!((c>64 && c<91) || (c>96 && c<123))) {
                word_start++
                break
              }
              word_start--
            }
            var input_word = this.input.value.substring(word_start, this.input.selectionEnd)

            // check for keyword match and insert
            // this just stops at the first match
            for(var keyword in keywords) {
              if(keyword.startsWith(input_word)){
                var completion = keyword.substring(input_word.length)
                this.input.value += completion
                var pos = this.input.value.length - keywords[keyword]
                this.input.setSelectionRange(pos, pos)
                break
              }
            }
          }
        })
        this.button.onclick = function() {
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

        const repl = this
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

function setNav(e) {
  // toggle the nav buttons
  document.getElementsByClassName("nav-active")[0].classList.remove("nav-active")
  e.classList.add("nav-active")

  // toggle the main interface content divs
  document.getElementsByClassName("interface-active")[0].classList.remove("interface-active")
  name = e.id.substring(4)
  interface_el = document.getElementById('interface-' + name)
  interface_el.classList.add("interface-active")

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
    status_formatted = JSON.stringify(JSON.parse(xhr.responseText), null, 4)
    status_node.innerHTML = status_formatted
    time_node.innerHTML = new Date().today() + " " + new Date().timeNow()
  }
  xhr.send(null)
}

function open_external_docs() {
  window.open("https://www.pilosa.com/docs");
}

Date.prototype.today = function () { 
    return this.getFullYear() +"/"+ (((this.getMonth()+1) < 10)?"0":"") + (this.getMonth()+1) +"/"+ ((this.getDate() < 10)?"0":"") + this.getDate();
}

Date.prototype.timeNow = function () {
     return ((this.getHours() < 10)?"0":"") + this.getHours() +":"+ ((this.getMinutes() < 10)?"0":"") + this.getMinutes() +":"+ ((this.getSeconds() < 10)?"0":"") + this.getSeconds();
}

populate_version()

const input = document.getElementById('query')
const output = document.getElementById('outputs')
const button = document.getElementById('query-btn')

repl = new REPL(input, output, button)
repl.populate_index_dropdown()
repl.bind_events()

input.focus()
