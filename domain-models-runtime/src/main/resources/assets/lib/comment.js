          // tutorial1.js
          window.CommentBox = React.createClass({
            render: function() {
              return (
                <div className="commentBox">
                 {this.props.label} <input type="text" name={this.props.fieldname}/><br/>
                </div>
              );
            }
          });
