
	var options = [
		{ label: 'One', value: 1 },
		{ label: 'Two', value: 2 },
		{ label: 'Three', value: 3 },
	];
	window.Container = React.createClass({
		getInitialState () {
			return { value: '' };
		},
		updateValue (value) {
			this.setState({ value: value });
		},
		render () {
			return React.createElement(Select, {
				options: options,
				onChange: this.updateValue,
				value: this.state.value,
			});
		}
	});
