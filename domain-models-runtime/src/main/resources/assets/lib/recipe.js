

        var Flux = new McFly();

        /** Store */

        var _recipes = [];

        function addRecipe(text){
            _recipes.push(text);
        }

        var RecipeStore = Flux.createStore({
            getRecipes: function(){
               return _recipes;
            }
        }, function(payload){//function registry
            if(payload.actionType === "ADD_RECIPE") {
                addRecipe(payload.text);
                RecipeStore.emitChange();
            }
        });

        /** Actions */
        /* when function from the Actions factory is triggered it will create a call to the function registry (in the store) 
        	proxy the data to the store*/
        var RecipeActions = Flux.createActions({
            addRecipe: function(text){
               return {
                  actionType: "ADD_RECIPE",
                  text: text
               }
            }
        });


        /** Controller View */

        var RecipesController = React.createClass({
            mixins: [RecipeStore.mixin], //the RecipeStore functions are now imported into the controller
            getInitialState: function(){ //
                return getRecipes();
            },
            storeDidChange: function() {
                this.setState(getRecipes());
            },
            render: function() {
                return <Recipes recipes={this.state.recipes} />;
            }
        });


	var _log = function(methodName, args) {
    	console.log(methodName, args);
  	};
	var RecipeImage = React.createClass({

		render: function(){
			var divStyle = {
				width: "100px",
				height: "100px"
			}
			return (
				<div>
					<img style={divStyle} src={this.props.url}></img>
				</div>

			);
		}

	});

	var Recipe = React.createClass({

	 	getInitialState : function() {
          return {
            dropDownIsOpen : false,
            hover: false,
          };
        },

        handleClick : function() {
          _log("handleClick", arguments);
          this.setState({
            dropDownIsOpen : !this.state.dropDownIsOpen
          });
        },
        mouseOver: function () {
        	//_log("mouseOver", arguments);
	        this.setState({hover: true});
	    },
	    mouseOut: function () {
	    	//_log("mouseOut", arguments);
	        this.setState({hover: false});
	    },
        renderDropdown: function () {
          return (
            <div style={{ background: "#ccc" }}>
              I am a dropdown!
            </div>
          );
        },

		  render: function() {

		  	var hov  = this.state.hover ? this.renderDropdown() : null;
		  	//var hov2 = this.state.hover ? this.renderDropdown() : null;
		    return (
		      <div key={this.props.key} onClick={this.handleClick} onMouseOver={this.mouseOver} onMouseOut={this.mouseOut}>
		      	<RecipeImage url={this.props.url} />
		      	<span>{this.props.key+1} </span>
		        <h2> {this.props.title} </h2>
		        <p> {this.props.instructions} </p>
		        <div>
		        	{hov}
		        </div>
		      </div>
		    );
		  }
	});

	var RecipeList = React.createClass({

	 	getInitialState : function() {
          return {
            value : "foo",
            data: []
          };
        },
        componentDidMount: function() {
            console.log("aaaaaaaaa");
            console.log(this.props.url);

            $.ajax({
              url: this.props.url,
              dataType: 'json',
              cache: false,
              success: function(data) {
                this.setState({data: data});
              }.bind(this),
              error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
              }.bind(this)
            });
          },
        addRecipe: function(){
           // RecipeActions.addRecipe({_id: Math.floor(Math.random()*1000000)});
            $.ajax({
                url: "http://localhost:8182/api/whiskies/0",
                dataType: 'json',
                cache: false,
                success: function(data) {
                  this.setState({data: data});
                }.bind(this),
                error: function(xhr, status, err) {
                  console.error(this.props.url, status, err.toString());
                }.bind(this)
              });
        },
	  render: function() {
	  	var recipeNodes = this.state.data.map(function(recipe, index){
	    	/**
	    	* each item in a collection needs a key... see:
				* http://facebook.github.io/react/docs/multiple-components.html#dynamic-children
	    	*/
	    	return (
	    		<div>     
	    		<Recipe
	    			key={index}
	    			title={recipe.name}
	    			instructions={recipe.origin} 
	    			url="https://mathemagicalsite.files.wordpress.com/2015/08/cookbook.gif?w=441&h=450"/>
	    		</div>
	    	)
	    })
	    return (
	      <div>
	      	<span>{this.state.value} </span>           
	      	RecipeList +>
	      	{recipeNodes}
            <button onClick={this.addRecipe}>Add</button>
	      </div>
	    );

	  }
	});

	var RecipeForm = React.createClass({
	  render: function() {
	    return (
	      <div>
	        RecipeForm component text
	      </div>
	    );
	  }
	});

	window.RecipeBook = React.createClass({
	  render: function() {
	    /* component composition == function componsition */
	    return (
	      <div>
	        Hello, world! I am a RecipeBook.
	        <RecipeList url={this.props.url}/>
	        <RecipeForm/>
	      </div>
	    );
	  }
	});
	window.recipeData = [
		{title: "Stuffed Chard", instructions: "Stuff the chard..."},
		{title: "Eggplant and Polenta", instructions: "Put the eggplant in the oven..."}
	];

