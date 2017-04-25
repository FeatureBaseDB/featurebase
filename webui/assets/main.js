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
          ENTER: 13,
          UP_ARROW: 38,
          DOWN_ARROW: 40
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
        var dbname = 'foo'; // todo: get db name from dropdown menu
        xhr.open('POST', '/db/' + dbname + '/query');
        xhr.setRequestHeader('Content-Type', 'application/text');

        const repl = this
        xhr.onload = function() {
            repl.result_number++
            repl.createSingleOutput({"input": query, "output": xhr.responseText})
        }
        xhr.send(query)
    }

    createSingleOutput(res) {
      var node = document.createElement("div");
      node.classList.add('output');
      var markup =`
        <div  class="panes">
          <div class="pane active">
            <div class="result-io">
              <div class="result-io-header">
                <h5>Input</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>Source: Index</em>
              </div>
              <div class="result-input">
                ${res.input}
              </div>
            </div>
            <div class="result-io">
              <div class="result-io-header">
                <h5>output</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>.42 ms</em>
              </div>
              <div class="result-ouput">
                ${res.output}
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
}

const input = document.getElementById('query')
const output = document.getElementById('outputs')
const button = document.getElementById('query-btn')

repl = new REPL(input, output, button)
repl.bind_events()
